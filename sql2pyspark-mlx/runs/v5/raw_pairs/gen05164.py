from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("session_id") == 19)
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_count"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_stats.alias("session_stats"), F.col("sessions.id") == F.col("session_stats.session_id"), "left")
    .filter(F.col("session_stats.page_view_count") > 4)
    .select(
        F.col("sessions.id"),
        F.col("sessions.browser"),
        F.col("sessions.device"),
        F.col("session_stats.page_view_count"),
        F.col("session_stats.avg_duration_sec"),
    )
    .orderBy("page_view_count")
)
