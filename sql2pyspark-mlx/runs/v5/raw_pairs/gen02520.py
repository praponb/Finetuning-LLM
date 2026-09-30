from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "right")
    .select(
        F.col("web_events.id"),
        F.col("web_events.event_time"),
        F.col("web_events.duration_sec"),
        F.col("web_events.event_type"),
        F.col("sessions.device"),
        F.col("sessions.browser"),
        F.col("sessions.country"),
    )
)
