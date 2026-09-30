from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("sessions.id") == F.col("web_events.session_id"), "inner")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "outer")
    .select(
        F.col("web_events.id"),
        F.col("web_events.event_time"),
        F.col("sessions.device"),
        F.col("customers.name"),
        F.col("customers.age"),
    )
)
