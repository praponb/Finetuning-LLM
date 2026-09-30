from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .select(F.col("s.device"), F.col("s.customer_id"), F.col("ss.min_duration_sec"))
    .orderBy(F.col("customer_id").desc())
)
