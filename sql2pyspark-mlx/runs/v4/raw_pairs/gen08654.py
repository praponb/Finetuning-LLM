from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("session_id").isin([27, 33]))
    .groupBy("session_id")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.stddev_duration_sec") >= 22)
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.country"),
        F.col("ss.stddev_duration_sec"),
        F.col("ss.max_duration_sec"),
    )
    .orderBy(F.col("country").desc(), "id")
    .limit(50)
)
