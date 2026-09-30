from pyspark.sql import functions as F

recent_orders = df_orders.filter(F.col("customer_id") == 12)

orders_stats = (
    recent_orders
    .groupBy("status")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = orders_stats.orderBy("status")
