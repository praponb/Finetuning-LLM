from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").desc_nulls_first())
w2 = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc_nulls_last())
w3 = Window.partitionBy("category", "supplier_id").orderBy(F.col("row_num").desc(), "id")
w4 = Window.partitionBy("supplier_id").orderBy("stock_qty_times_cost", "id")
w5 = Window.partitionBy("supplier_id", "category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_products
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

deduped = (
    latest
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") < F.col("s.launch_date")), "left_semi")
)

ranked = (
    not_flagged
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

filtered = (
    ranked
    .filter(F.array_contains("tags", "clearance") & F.col("supplier_id").isNotNull())
    .select(
        "*",
        F.concat("brand", F.lit(", "), "category").alias("brand_category"),
        (F.col("stock_qty") * F.col("cost")).alias("stock_qty_times_cost"),
        F.coalesce("brand", F.lit("N/A")).alias("resolved_brand"),
    )
)

enriched = (
    filtered
    .withColumn("next_cost", F.lead("cost", 1).over(w4))
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w5))
)

by_type = (
    enriched
    .groupBy("category")
    .pivot("brand", ["Acme", "Umbrella"])
    .agg(F.avg("price"))
    .select(
        "category",
        F.col("Acme").alias("acme"),
        F.col("Umbrella").alias("umbrella"),
    )
)

result = (
    by_type
    .select("category", "umbrella")
    .orderBy(F.col("category").asc_nulls_last())
)
