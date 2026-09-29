from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category", "brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("price_int", "id")
w4 = Window.partitionBy("brand")

base = (
    df_products
    .filter((~F.col("brand").eqNullSafe(F.lit("Globex"))) & F.col("brand").isin(["Umbrella", "Stark"]))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        F.col("price").cast("int").alias("price_int"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_clean"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.last("price", ignorenulls=True).over(w).alias("last_known_price"),
        F.last("brand", ignorenulls=True).over(w2).alias("last_known_brand"),
        F.row_number().over(w_ordered).alias("event_seq"),
        F.corr("cost", "price").over(w4).alias("corr_cost_price"),
    )
)

valid = (
    enriched
    .filter((F.col("launch_date") > F.lit("2023-09-03").cast("date")) & F.col("brand").isNotNull())
    .withColumn("cost_per_corr_cost_price", (F.col("cost") / F.nullif("corr_cost_price", F.lit(0))))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("OTHER")))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.sale_date").between("2024-01-01", "2024-10-01")), "left_semi")
)

result = (
    not_flagged
    .cube("brand")
    .agg(
        F.count("*").alias("row_count"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.round(F.avg("cost_per_corr_cost_price"), 2).alias("avg_cost_per_corr_cost_price"),
        F.grouping("brand").alias("grouping_id"),
    )
    .filter(F.col("avg_cost_per_corr_cost_price") > 0)
    .orderBy(F.col("brand").desc())
)
