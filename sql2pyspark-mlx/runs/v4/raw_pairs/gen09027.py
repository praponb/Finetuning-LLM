from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("sessions.id") == F.col("web_events.session_id"), "inner")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("sessions.country") != "JP")
    .select(
        F.col("web_events.id"),
        F.col("web_events.duration_sec"),
        F.col("sessions.started_at"),
        F.col("customers.city"),
        F.col("customers.is_active"),
    )
)
