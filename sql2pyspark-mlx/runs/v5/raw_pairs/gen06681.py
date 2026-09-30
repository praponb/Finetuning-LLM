from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("event_time") < "2024-08-01")
    .groupBy("session_id")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .select(
        F.col("s.browser"),
        F.col("s.device"),
        F.col("ss.stddev_duration_sec"),
        F.col("ss.row_count"),
    )
    .orderBy("browser")
)
