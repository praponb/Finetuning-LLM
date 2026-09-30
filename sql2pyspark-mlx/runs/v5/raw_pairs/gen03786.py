from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("page") == "/checkout")
    .groupBy("session_id")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("t.stddev_duration_sec"),
        F.col("t.avg_duration_sec"),
    )
    .orderBy("avg_duration_sec")
)
