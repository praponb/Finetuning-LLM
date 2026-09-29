from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("duration_sec").isNotNull())
    .groupBy("session_id")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_stats.alias("t"), F.col("sessions.id") == F.col("t.session_id"), "left")
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("t.max_duration_sec"),
        F.col("t.stddev_duration_sec"),
    )
)
