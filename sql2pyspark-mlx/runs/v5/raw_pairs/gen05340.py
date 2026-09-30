from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.started_at") >= "2024-01-01")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("s.started_at"),
        F.col("c.email"),
        F.col("c.city"),
        F.col("c.age"),
    )
)
