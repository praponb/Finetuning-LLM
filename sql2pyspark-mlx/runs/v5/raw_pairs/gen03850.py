from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .filter(F.col("ss.add_to_cart_count") > 6)
    .select(F.col("s.id"), F.col("s.browser"), F.col("ss.add_to_cart_count"))
    .orderBy(F.col("add_to_cart_count").desc())
)
