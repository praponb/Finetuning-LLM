from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .filter(F.col("sessions.device") == "tablet")
    .select(
        F.col("web_events.session_id"),
        F.col("web_events.event_type"),
        F.col("web_events.duration_sec"),
        F.col("sessions.country"),
        F.col("sessions.browser"),
        F.col("sessions.started_at"),
    )
    .orderBy(F.col("country").desc(), "started_at")
)
