from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.to_date("event_time") < "2025-05-01")
    .groupBy("session_id")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(
        F.col("s.started_at"),
        F.col("t.duration_sec_range"),
        F.col("t.avg_duration_sec"),
    )
    .orderBy("started_at")
)
