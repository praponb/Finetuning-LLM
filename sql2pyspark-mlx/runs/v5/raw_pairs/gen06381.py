from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.customer_id").isNotNull())
    .select(
        F.col("we.session_id"),
        F.col("we.event_type"),
        F.col("we.duration_sec"),
        F.col("s.started_at"),
        F.col("s.customer_id"),
    )
)
