from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("duration_sec") >= 410)
    .groupBy("session_id")
    .agg(F.count("*").alias("web_event_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.web_event_count") >= 9)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.browser"),
        F.col("t.web_event_count"),
    )
)
