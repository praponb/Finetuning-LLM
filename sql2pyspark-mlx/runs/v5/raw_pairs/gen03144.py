from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .filter(F.col("sessions.device") == "desktop")
    .groupBy(F.col("sessions.customer_id"))
    .agg(F.round(F.avg(F.col("web_events.duration_sec")), 2).alias("avg_duration_sec"))
    .filter(F.col("avg_duration_sec") > 54)
)
