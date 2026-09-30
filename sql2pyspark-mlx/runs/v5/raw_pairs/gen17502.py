from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"))
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("customer_stats"), F.col("customers.id") == F.col("customer_stats.customer_id"), "left")
    .select(
        F.col("customers.age"),
        F.col("customers.name"),
        F.col("customer_stats.mobile_count"),
    )
    .orderBy("mobile_count")
)
