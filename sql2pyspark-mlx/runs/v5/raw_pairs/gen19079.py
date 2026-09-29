from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("page") != "/cart")
    .groupBy("session_id")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.min_duration_sec") >= 410)
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("t.max_duration_sec"),
        F.col("t.min_duration_sec"),
    )
)
