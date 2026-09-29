from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("session_id").isin([2, 11, 16, 27]))
    .groupBy("session_id")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.total_duration_sec") >= 1800)
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("ss.total_duration_sec"),
    )
)
