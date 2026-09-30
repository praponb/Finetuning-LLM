from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price_whole").asc_nulls_first(), "id")

cleaned = (
    df_order_items
    .filter(F.col("quantity").between(4, 7))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.ceil("unit_price").alias("unit_price_whole"),
    )
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

valid = (
    ranked
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("quantity") >= 9, "LARGE").otherwise("SMALL").alias("quantity_tier"),
        "product_id",
    )
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.corr("unit_price", "quantity").alias("corr_unit_price_quantity"),
        F.round(F.avg("unit_price_whole"), 2).alias("avg_unit_price_whole"),
        F.count("*").alias("row_count"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.percentile_approx("unit_price_whole", 0.95).alias("unit_price_whole_p95"),
    )
    .orderBy(F.col("corr_unit_price_quantity").asc_nulls_last())
)
