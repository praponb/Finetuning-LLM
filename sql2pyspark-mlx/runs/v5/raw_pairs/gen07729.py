from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("S"), F.col("s.id") == F.col("S.session_id"), "left")
    .filter(F.col("S.stddev_duration_sec") > 68)
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("S.stddev_duration_sec"),
        F.col("S.avg_duration_sec"),
    )
    .orderBy("started_at")
)
