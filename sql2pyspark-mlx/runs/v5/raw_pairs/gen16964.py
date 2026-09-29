from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.count("*").alias("session_count"))
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("c.is_active"),
        F.col("t.session_count"),
    )
    .orderBy(F.col("session_count").desc())
)
