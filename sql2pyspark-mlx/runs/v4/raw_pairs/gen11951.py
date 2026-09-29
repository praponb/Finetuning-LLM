from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("webs.first_event_time"),
        F.col("webs.duration_sec_count"),
    )
    .orderBy(F.col("id").desc())
)
