from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id") == 36) & F.col("quantity").isNotNull())
    .groupBy("order_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
    .orderBy(F.col("order_id").desc(), F.col("avg_quantity").desc())
    .limit(10)
)
