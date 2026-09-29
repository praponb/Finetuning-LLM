from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .filter(F.col("web_events.duration_sec") < 416)
    .select(
        F.col("sessions.started_at"),
        F.col("web_events.session_id"),
        F.col("web_events.event_time"),
    )
)
