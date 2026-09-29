from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "right")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .groupBy(
        F.date_format(F.col("customers.signup_date"), "yyyy-MM").alias("signup_month"),
        F.col("customers.is_active"),
    )
    .agg(F.countDistinct(F.col("web_events.duration_sec")).alias("unique_event_types"))
)
