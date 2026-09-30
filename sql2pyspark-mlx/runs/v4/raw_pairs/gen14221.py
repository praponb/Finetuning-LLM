from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "inner")
    .filter(F.lower(F.col("customers.name")).like("%mma%"))
    .select(
        F.col("web_events.id"),
        F.col("web_events.duration_sec"),
        F.col("sessions.country"),
        F.col("customers.email"),
    )
    .orderBy(F.col("id").desc(), F.col("duration_sec").desc())
)
