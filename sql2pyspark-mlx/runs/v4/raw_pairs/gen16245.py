from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id").isin([5, 36]) & F.col("product_id").isNull()) | (F.col("order_id") == 12))
    .groupBy("product_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.sum("quantity").alias("total_quantity"),
        F.avg("quantity").alias("avg_quantity"),
        F.count("unit_price").alias("unit_price_count"),
    )
    .filter(F.col("unit_price_count") >= 5)
    .drop("unit_price_count")
)
