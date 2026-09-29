from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter((F.length(F.col("c.name")) < 9) & (F.col("c.age") != 67))
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.customer_id"),
        F.col("s.country"),
        F.col("c.is_active"),
        F.col("c.email"),
    )
)
