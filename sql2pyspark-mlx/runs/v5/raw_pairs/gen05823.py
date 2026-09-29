from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.hour("event_time").between(6, 10))
    .groupBy("session_id")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.total_duration_sec") > 2600)
    .select(F.col("s.id"), F.col("s.country"), F.col("t.total_duration_sec"))
    .orderBy(F.col("id").desc())
)
