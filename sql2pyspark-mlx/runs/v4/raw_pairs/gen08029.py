from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "outer")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "right")
    .select(
        F.col("web_events.event_time"),
        F.col("sessions.browser"),
        F.col("sessions.device"),
        F.col("customers.segment"),
    )
)
