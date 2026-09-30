from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("event_type") == "add_to_cart")
    .groupBy("session_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("t.total_duration_sec"),
        F.col("t.min_duration_sec"),
    )
)
