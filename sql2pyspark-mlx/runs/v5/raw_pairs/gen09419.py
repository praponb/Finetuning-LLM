from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("duration_sec").between(195, 437))
    .groupBy("session_id")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("S"), F.col("s.id") == F.col("S.session_id"), "left")
    .filter(F.col("S.avg_duration_sec") >= 410)
    .select(F.col("s.device"), F.col("s.browser"), F.col("S.avg_duration_sec"))
    .orderBy("device")
)
