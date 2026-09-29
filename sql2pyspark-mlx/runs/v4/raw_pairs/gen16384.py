from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("click_count"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(
        F.col("s.browser"),
        F.col("s.country"),
        F.col("t.click_count"),
        F.col("t.total_duration_sec"),
    )
)
