from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter(F.col("amount") > 1110)
    .withColumn("amount_whole", F.floor("amount"))
)

orders_stats = (
    recent_orders
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"))
)

result = (
    orders_stats
    .filter(F.col("cancelled_count") < 8)
    .orderBy(F.col("order_year").desc())
)
