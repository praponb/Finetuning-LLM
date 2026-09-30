from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("event_type").isin(["click", "purchase", "add_to_cart"]))
    .groupBy("session_id")
    .agg(F.sort_array(F.collect_set("page")).alias("page_list"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(F.col("s.browser"), F.col("s.customer_id"), F.col("t.page_list"))
)
