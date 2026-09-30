from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("sessions.id") == F.col("web_events.session_id"), "left")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .filter((F.col("sessions.country") == "US") & F.col("web_events.duration_sec").between(212, 294))
    .select(
        F.col("web_events.id"),
        F.col("web_events.session_id"),
        F.col("web_events.event_time"),
        F.col("sessions.customer_id"),
        F.col("sessions.browser"),
        F.col("customers.signup_date"),
        F.col("customers.credit_limit"),
    )
)
