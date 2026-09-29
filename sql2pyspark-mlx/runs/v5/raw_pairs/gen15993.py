from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(~F.col("event_type").isin(["page_view", "purchase", "click"]))
    .groupBy("session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0))) / F.count("*")), 1).alias("pct_purchase"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_count"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_summary.alias("session_summary"), F.col("sessions.id") == F.col("session_summary.session_id"), "inner")
    .filter(F.col("session_summary.search_count") > 6)
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("sessions.device"),
        F.col("session_summary.pct_purchase"),
        F.col("session_summary.search_count"),
    )
    .orderBy(F.col("id").desc())
)
