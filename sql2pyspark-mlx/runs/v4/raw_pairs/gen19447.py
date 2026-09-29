from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "inner")
    .filter(F.hour(F.col("sessions.started_at")).between(10, 16) & (F.col("web_events.event_type") == "purchase"))
    .select(
        F.col("sessions.started_at"),
        F.col("sessions.customer_id"),
        F.col("web_events.session_id"),
        F.col("web_events.page"),
        F.col("web_events.event_time"),
    )
    .orderBy("started_at")
)
