from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(
        F.col("s.country"),
        F.col("t.avg_duration_sec"),
        F.col("t.stddev_duration_sec"),
    )
    .orderBy(F.col("stddev_duration_sec").desc())
)
