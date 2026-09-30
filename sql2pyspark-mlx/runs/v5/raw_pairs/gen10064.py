from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter(F.col("customer_id").isin([2, 15, 16, 19]) & (F.col("status") == "PENDING") & F.col("status").isin(["COMPLETED", "SHIPPED"]))
    .withColumn("amount_with_shipping", (F.col("amount") - F.col("shipping_fee")))
)

orders_stats = (
    filtered_orders
    .groupBy("status")
    .agg(F.count("*").alias("order_count"))
)

result = (
    orders_stats
    .select("status", "order_count")
    .orderBy("status", F.col("order_count").desc())
    .limit(50)
)
