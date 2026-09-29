from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(F.max("duration_sec").alias("max_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .filter(F.col("t.max_duration_sec") >= 290)
    .select(F.col("s.id"), F.col("s.customer_id"), F.col("t.max_duration_sec"))
)
