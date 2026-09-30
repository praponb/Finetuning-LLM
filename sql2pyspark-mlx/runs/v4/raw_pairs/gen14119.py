from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .filter(F.col("country") == "TH")
    .groupBy("customer_id")
    .agg(F.min("started_at").alias("first_started_at"))
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("S"), F.col("c.id") == F.col("S.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("c.credit_limit"),
        F.col("S.first_started_at"),
    )
    .orderBy("first_started_at")
)
