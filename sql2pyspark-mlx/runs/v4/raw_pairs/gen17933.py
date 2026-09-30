from pyspark.sql import functions as F

base = df_orders.filter((F.col("customer_id") == 9) & (F.col("discount") <= 0.25))

orders_stats = (
    base
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    orders_stats
    .filter(F.col("avg_amount") >= 31)
    .select("order_quarter", "avg_amount")
    .orderBy("avg_amount", "order_quarter")
    .limit(5)
)
