from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([17, 24, 26, 36]) & F.col("product_id").isNull())
    .groupBy("product_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
)
