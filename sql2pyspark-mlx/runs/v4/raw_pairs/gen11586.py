from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("duration_sec").between(163, 475))
    .groupBy("session_id")
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.total_duration_sec") > 1000)
    .select(
        F.col("s.device"),
        F.col("s.started_at"),
        F.col("t.duration_sec_count"),
        F.col("t.total_duration_sec"),
    )
)
