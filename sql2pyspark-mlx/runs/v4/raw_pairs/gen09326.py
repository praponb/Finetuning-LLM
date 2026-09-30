from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([5, 10, 12, 34]))
    .groupBy("order_id")
    .agg(F.avg("unit_price").alias("avg_unit_price"))
    .filter(F.col("avg_unit_price") >= 260)
    .orderBy("order_id", F.col("avg_unit_price").desc())
)
