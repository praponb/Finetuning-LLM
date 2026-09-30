from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("sessions.id") == F.col("web_events.session_id"), "inner")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .groupBy(F.col("sessions.browser"))
    .agg(
        F.sum(F.when(F.col("web_events.event_type") == "click", 1).otherwise(0)).alias("click_count"),
        F.max(F.col("web_events.duration_sec")).alias("max_duration_sec"),
    )
    .orderBy(F.col("click_count").desc())
)
