from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter(F.col("event_type").isin(["add_to_cart", "page_view"]) | (F.col("event_time") >= "2024-10-01"))
)

web_events_stats = (
    recent_web_events
    .groupBy(
        "event_type",
        F.when(F.col("duration_sec") >= 411, "high").when(F.col("duration_sec") >= 137, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_add_to_cart"),
        F.sum(F.when(F.col("event_type") == "search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
)

result = (
    web_events_stats
    .filter(F.col("search_duration_sec") >= 68)
    .select(
        "event_type",
        "duration_sec_bucket",
        "pct_add_to_cart",
        "search_duration_sec",
    )
)
