from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("browser") == "Edge", 1).otherwise(0)).alias("edge_count"),
        F.min("started_at").alias("min_started_at"),
    )
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "left")
    .select(
        F.col("c.signup_date"),
        F.col("c.is_active"),
        F.col("sbc.edge_count"),
        F.col("sbc.min_started_at"),
    )
    .orderBy("signup_date")
)
