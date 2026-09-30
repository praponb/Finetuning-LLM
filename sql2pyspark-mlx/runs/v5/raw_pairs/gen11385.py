from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.name").isNull())
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("we.event_time"),
        F.col("s.device"),
        F.col("s.customer_id"),
        F.col("c.email"),
    )
)
