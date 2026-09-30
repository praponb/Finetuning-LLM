from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("duration_sec") > 412)
    .groupBy("session_id")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_stats.alias("session_stats"), F.col("sessions.id") == F.col("session_stats.session_id"), "inner")
    .filter(F.col("session_stats.max_duration_sec") > 220)
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("sessions.customer_id"),
        F.col("session_stats.avg_duration_sec"),
        F.col("session_stats.max_duration_sec"),
    )
    .orderBy("customer_id")
)
