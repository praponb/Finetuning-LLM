from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "right")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.hour(F.col("web_events.event_time")).between(11, 15) & (F.col("customers.country") == "US"))
    .select(
        F.col("web_events.id"),
        F.col("web_events.duration_sec"),
        F.col("web_events.event_type"),
        F.col("sessions.country"),
        F.col("customers.age"),
        F.col("customers.segment"),
    )
    .orderBy("age")
)
