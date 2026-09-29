from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"))
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "left")
    .select(F.col("c.country"), F.col("sbc.mobile_count"))
)
