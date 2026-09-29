from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.min("event_time").alias("min_event_time"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "left")
    .filter(F.col("webs.avg_duration_sec") > 75)
    .select(
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("webs.avg_duration_sec"),
        F.col("webs.min_event_time"),
    )
)
