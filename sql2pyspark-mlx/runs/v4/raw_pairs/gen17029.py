from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "outer")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.city") == "Chiang Mai")
    .select(
        F.col("we.id"),
        F.col("we.duration_sec"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("c.city"),
        F.col("c.is_active"),
        F.when(F.col("c.credit_limit") >= 12200, "High").when(F.col("c.credit_limit") >= 8200, "Medium").otherwise("Low").alias("credit_limit_tier"),
    )
)
