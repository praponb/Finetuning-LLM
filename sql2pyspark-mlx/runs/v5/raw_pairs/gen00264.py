from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("duration_sec") > 163)
    .groupBy("session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_add_to_cart"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("S"), F.col("s.id") == F.col("S.session_id"), "inner")
    .filter(F.col("S.pct_add_to_cart") >= 31)
    .select(F.col("s.started_at"), F.col("S.pct_add_to_cart"), F.col("S.pct_search"))
)
