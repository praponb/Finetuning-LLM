from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(~F.col("event_type").isin(["add_to_cart", "purchase"]))
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("ss.event_type_values"),
        F.col("ss.page_values"),
    )
)
