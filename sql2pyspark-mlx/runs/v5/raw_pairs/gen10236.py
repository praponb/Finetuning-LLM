from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
)

result = (
    df_sessions.alias("sessions")
    .join(session_summary.alias("session_summary"), F.col("sessions.id") == F.col("session_summary.session_id"), "left")
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("session_summary.avg_duration_sec"),
    )
    .orderBy("id")
)
