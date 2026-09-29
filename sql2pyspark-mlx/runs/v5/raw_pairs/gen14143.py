from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("duration_sec") >= 477)
    .groupBy("session_id")
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(F.col("s.id"), F.col("s.device"), F.col("t.avg_duration_sec"))
    .orderBy(F.col("avg_duration_sec").desc())
)
