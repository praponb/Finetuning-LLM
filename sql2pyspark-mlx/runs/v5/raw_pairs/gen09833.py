from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("duration_sec") != 411)
    .groupBy("session_id")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .filter(F.col("t.avg_duration_sec") >= 140)
    .select(F.col("s.id"), F.col("s.browser"), F.col("t.avg_duration_sec"))
    .orderBy("browser")
)
