from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id").isin([1, 8, 13, 30]) & (F.col("unit_price") < 100)) | (F.col("order_id") == 32))
    .groupBy("order_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.min("quantity").alias("min_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
    .orderBy("order_id")
)
