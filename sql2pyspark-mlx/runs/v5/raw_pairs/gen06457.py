from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .filter(F.col("country") != "DE")
    .groupBy("customer_id")
    .agg(
        F.max("started_at").alias("last_started_at"),
        F.min("started_at").alias("first_started_at"),
    )
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "left")
    .select(
        F.col("c.signup_date"),
        F.col("sbc.last_started_at"),
        F.col("sbc.first_started_at"),
    )
)
