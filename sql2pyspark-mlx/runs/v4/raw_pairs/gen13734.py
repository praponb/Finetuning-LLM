from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(F.col("c.segment"), F.col("cs.mobile_count"))
)
