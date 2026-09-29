from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.max("event_time").alias("latest_event_time"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .filter(F.col("ss.checkout_count") >= 7)
    .select(
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("ss.checkout_count"),
        F.col("ss.latest_event_time"),
    )
    .orderBy("browser")
)
