from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("we.event_time") < "2024-02-01")
    .select(
        F.col("s.country"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("we.event_time"),
        F.col("we.session_id"),
        F.col("we.event_type"),
    )
    .orderBy("event_time")
)
