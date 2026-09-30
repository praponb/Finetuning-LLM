from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.duration_sec_count") > 8)
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.started_at"),
        F.col("ss.total_duration_sec"),
        F.col("ss.duration_sec_count"),
    )
    .orderBy(F.col("duration_sec_count").desc(), "id")
    .limit(10)
)
