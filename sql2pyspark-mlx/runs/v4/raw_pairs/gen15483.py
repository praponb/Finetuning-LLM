from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(F.max("duration_sec").alias("max_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "left")
    .filter(F.col("webs.max_duration_sec") >= 300)
    .select(F.col("s.id"), F.col("s.browser"), F.col("webs.max_duration_sec"))
    .orderBy(F.col("id").desc())
)
