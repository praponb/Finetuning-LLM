from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .filter(F.col("webs.duration_sec_count") >= 4)
    .select(
        F.col("s.country"),
        F.col("s.device"),
        F.col("webs.total_duration_sec"),
        F.col("webs.duration_sec_count"),
    )
)
