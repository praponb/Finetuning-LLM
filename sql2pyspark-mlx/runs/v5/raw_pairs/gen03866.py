from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "right")
    .filter((F.col("web_events.event_type") != "add_to_cart") | (F.col("web_events.event_time") < "2023-10-01"))
    .select(
        F.col("web_events.id"),
        F.col("web_events.session_id"),
        F.col("sessions.device"),
        F.col("sessions.started_at"),
        F.trim(F.col("sessions.country")).alias("clean_country"),
    )
)
