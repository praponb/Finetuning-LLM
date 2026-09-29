from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .select(
        F.col("web_events.id"),
        F.col("web_events.event_time"),
        F.col("web_events.event_type"),
        F.col("web_events.page"),
        F.col("sessions.country"),
        F.col("sessions.customer_id"),
        F.col("sessions.started_at"),
    )
)
