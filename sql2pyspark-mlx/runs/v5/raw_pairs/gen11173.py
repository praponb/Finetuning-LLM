from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(~(F.col("we.duration_sec").isNotNull() | (F.col("s.started_at") >= "2024-05-01")))
    .select(
        F.col("we.event_time"),
        F.col("we.duration_sec"),
        F.col("we.event_type"),
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("s.country"),
    )
    .orderBy("event_type")
)
