from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.to_date("event_time") >= "2024-08-13")
    .groupBy("session_id")
    .agg(
        F.count("*").alias("web_event_count"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .filter(F.col("ss.web_event_count") > 9)
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("ss.web_event_count"),
        F.col("ss.total_duration_sec"),
    )
    .orderBy(F.col("id").desc(), "web_event_count")
)
