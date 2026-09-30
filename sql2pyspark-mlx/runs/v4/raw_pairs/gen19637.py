from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.credit_limit") < 14900)
    .select(
        F.col("c.name"),
        F.col("c.country"),
        F.col("s.device"),
        F.col("s.browser"),
        F.col("s.started_at"),
    )
)
