from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.is_active") & F.col("c.email").like("%@yahoo.com"))
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("s.customer_id"),
        F.col("c.age"),
        F.col("c.city"),
        F.col("c.is_active"),
        F.length(F.col("c.country")).alias("country_length"),
    )
)
