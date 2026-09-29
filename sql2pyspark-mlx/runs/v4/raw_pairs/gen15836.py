from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .filter(F.col("t.total_duration_sec") >= 1900)
    .select(
        F.col("s.browser"),
        F.col("t.total_duration_sec"),
        F.col("t.avg_duration_sec"),
    )
)
