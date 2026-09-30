from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("we.duration_sec") == 408)
    .select(
        F.col("we.duration_sec"),
        F.col("we.session_id"),
        F.col("s.device"),
        F.col("s.browser"),
        F.col("c.is_active"),
        F.col("c.name"),
    )
)
