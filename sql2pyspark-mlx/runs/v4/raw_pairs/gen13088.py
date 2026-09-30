from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("c.age"),
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
    )
)
