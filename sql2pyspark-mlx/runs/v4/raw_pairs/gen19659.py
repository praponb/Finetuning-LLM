from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "outer")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.age").between(35, 50))
    .select(
        F.col("web_events.id"),
        F.col("web_events.event_time"),
        F.col("web_events.duration_sec"),
        F.col("sessions.customer_id"),
        F.col("sessions.country"),
        F.col("customers.signup_date"),
    )
)
