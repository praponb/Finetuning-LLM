from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .select(
        F.col("sessions.browser"),
        F.col("sessions.device"),
        F.col("sessions.started_at"),
        F.col("web_events.session_id"),
        F.col("web_events.page"),
        F.col("web_events.event_type"),
        F.lower(F.col("sessions.device")).alias("device_lower"),
    )
)
