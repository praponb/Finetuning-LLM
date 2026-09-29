from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("page") == "/checkout")
    .groupBy("session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_add_to_cart"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .select(
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("webs.pct_add_to_cart"),
    )
)
