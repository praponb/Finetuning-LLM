from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("cost_filled", "id")
w2 = Window.partitionBy("supplier_id", "brand").orderBy("cost", "id")
w_part = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("supplier_id", "brand")
w6 = Window.partitionBy("supplier_id").orderBy(F.col("prev_cost_filled").desc())
w7 = Window.orderBy(F.col("cost_filled").desc())

valid = (
    df_products
    .filter(F.col("launch_date") > F.to_date(F.lit("2023-11-06")))
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
        F.coalesce("brand", F.lit("NONE")).alias("resolved_brand"),
    )
)

enriched = (
    valid
    .withColumn("prev_cost_filled", F.lag("cost_filled", 1).over(w))
    .withColumn("next_cost", F.lead("cost", 1, 0).over(w2))
    .withColumn("cumulative_stock_qty", F.sum("stock_qty").over(w_part))
    .withColumn("first_known_brand", F.first("brand", ignorenulls=True).over(w_grp))
)

matched_keys = (
    enriched
    .select("brand", "id")
    .exceptAll(
        df_products
        .filter(F.upper("product_name").like("%MAR%"))
        .select("brand", "id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "*",
        F.stddev_samp("cost_filled").over(w5).alias("supplier_id_stddev_samp_cost_filled"),
        F.sum("cost").over(w5).alias("supplier_id_sum_cost"),
        F.percent_rank().over(w6).alias("prev_cost_filled_percent_rank"),
        F.cume_dist().over(w7).alias("cost_filled_cume_dist"),
    )
)

enriched_products = (
    with_windows
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "inner")
    .select("e.*", F.col("s.country"), F.col("s.city"))
)

result = (
    enriched_products
    .select(
        "category",
        F.col("cost_filled_cume_dist").alias("COST_FILLED_CUME_DIST"),
        F.col("cost_filled").alias("COST_FILLED"),
    )
    .unpivot(
        ["category"],
        ["COST_FILLED_CUME_DIST", "COST_FILLED"],
        "measure",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
