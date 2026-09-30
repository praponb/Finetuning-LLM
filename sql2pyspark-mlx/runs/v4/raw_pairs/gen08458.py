from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "page_view", F.col("duration_sec")).otherwise(0)).alias("page_view_duration_sec"),
        F.max("event_time").alias("last_event_time"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.page_view_duration_sec") >= 46)
    .select(
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("ss.page_view_duration_sec"),
        F.col("ss.last_event_time"),
    )
    .orderBy(F.col("last_event_time").desc())
)
