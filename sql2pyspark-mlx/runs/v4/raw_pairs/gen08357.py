from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0))) / F.count("*")), 1).alias("pct_page_view"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.pct_page_view") >= 54)
    .select(F.col("s.id"), F.col("s.started_at"), F.col("ss.pct_page_view"))
)
