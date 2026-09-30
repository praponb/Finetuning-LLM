from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w_part = Window.partitionBy("product_id").orderBy("stock_qty", "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-07-01").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.last_day("last_restock").alias("last_restock_month_end"),
        F.round("quantity", 0).alias("quantity_rounded"),
    )
)

enriched = (
    valid
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.cost"), F.col("d.stock_qty"))
)

scored = (
    enriched
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("prev_quantity", F.lag("quantity", 1).over(w_part))
)

stats = (
    scored
    .groupBy(
        F.when(F.col("quantity_rounded") >= 75, "HIGH").otherwise("LOW").alias("quantity_rounded_tier"),
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("prev_quantity"), 2).alias("avg_prev_quantity"),
    )
)

result = (
    stats
    .select(
        "quantity_rounded_tier",
        "distinct_product_ids",
        "num_rows",
        "avg_prev_quantity",
    )
    .orderBy(F.col("avg_prev_quantity").asc_nulls_first())
)
