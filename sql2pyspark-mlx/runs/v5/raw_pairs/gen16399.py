from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(~F.col("event_type").isin(["click", "search", "add_to_cart"]))
    .groupBy("session_id")
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .filter(F.col("ss.stddev_duration_sec") >= 56)
    .select(F.col("s.id"), F.col("s.device"), F.col("ss.stddev_duration_sec"))
    .orderBy(F.col("id").desc())
)
