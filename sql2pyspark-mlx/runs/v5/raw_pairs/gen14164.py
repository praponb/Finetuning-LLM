from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.is_active"))
    .select(
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("c.country"),
        F.col("c.credit_limit"),
        F.col("c.is_active"),
    )
)
