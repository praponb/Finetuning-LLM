from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .groupBy(F.col("sessions.device"))
    .agg(
        F.round(F.stddev(F.col("web_events.duration_sec")), 2).alias("stddev_duration_sec"),
        F.round(((100 * F.sum(F.when(F.col("web_events.event_type") == "click", 1).otherwise(0))) / F.count("*")), 1).alias("pct_click"),
    )
    .orderBy(F.col("pct_click").desc())
)
