from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
)

result = (
    df_sessions.alias("sessions")
    .join(session_summary.alias("t"), F.col("sessions.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.avg_duration_sec") >= 350)
    .select(
        F.col("sessions.id"),
        F.col("sessions.country"),
        F.col("sessions.device"),
        F.col("t.avg_duration_sec"),
    )
    .orderBy("country", "id")
    .limit(1)
)
