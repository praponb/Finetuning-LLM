from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(F.count("*").alias("order_count"))
    .orderBy(F.col("order_count").desc())
)
