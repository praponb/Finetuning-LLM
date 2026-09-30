from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("event_type") != "add_to_cart")
    .groupBy("session_id")
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.browser"),
        F.col("t.stddev_duration_sec"),
    )
)
