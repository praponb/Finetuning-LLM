from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.total_duration_sec") > 2400)
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("t.first_event_time"),
        F.col("t.total_duration_sec"),
    )
    .orderBy("first_event_time")
)
