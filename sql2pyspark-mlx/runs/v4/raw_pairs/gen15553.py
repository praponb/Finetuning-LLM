from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "outer")
    .filter((F.col("web_events.page") == "/checkout") & (F.col("web_events.duration_sec") <= 311))
    .select(
        F.col("web_events.id"),
        F.col("web_events.duration_sec"),
        F.col("web_events.event_type"),
        F.col("sessions.customer_id"),
    )
)
