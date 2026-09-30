from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("event_time") >= "2023-11-01")
    .groupBy("session_id")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.min_duration_sec") >= 180)
    .select(F.col("s.started_at"), F.col("ss.min_duration_sec"))
    .orderBy("started_at")
)
