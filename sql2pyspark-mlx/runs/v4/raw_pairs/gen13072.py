from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("event_type").isin(["add_to_cart", "click"]))
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/product", F.col("duration_sec")).otherwise(0)).alias("product_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .select(
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("ss.product_duration_sec"),
        F.col("ss.min_duration_sec"),
    )
    .orderBy("min_duration_sec")
)
