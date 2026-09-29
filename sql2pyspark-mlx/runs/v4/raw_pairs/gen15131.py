from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("session_id").isin([4, 25, 26]))
    .groupBy("session_id")
    .agg(
        F.count("*").alias("cnt"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_stats.alias("session_stats"), F.col("sessions.id") == F.col("session_stats.session_id"), "inner")
    .select(
        F.col("sessions.started_at"),
        F.col("sessions.device"),
        F.col("session_stats.cnt"),
        F.col("session_stats.total_duration_sec"),
    )
    .orderBy("device")
)
