from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.customer_id").isNotNull())
    .select(
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("c.segment"),
        F.col("c.signup_date"),
    )
)
