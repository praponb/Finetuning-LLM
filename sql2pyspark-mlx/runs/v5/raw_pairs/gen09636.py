from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("event_type") != "add_to_cart")
    .groupBy("session_id")
    .agg(F.count("duration_sec").alias("duration_sec_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.col("ss.duration_sec_count"),
    )
    .orderBy(F.col("duration_sec_count").desc())
)
