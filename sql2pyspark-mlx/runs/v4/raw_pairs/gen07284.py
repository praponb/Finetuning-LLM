from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .filter(F.year("end_date") >= 2023)
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("C"), F.col("c.id") == F.col("C.customer_id"), "left")
    .select(F.col("c.is_active"), F.col("C.cancelled_count"))
)
