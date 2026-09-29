from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter(F.col("page").isin(["/product", "/cart"]) & F.col("page").isin(["/product", "/home", "/checkout"]))
)

web_events_stats = (
    recent_web_events
    .groupBy(
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
        F.when(F.col("event_type").isin(["add_to_cart", "click"]), "priority").otherwise("other").alias("event_type_group"),
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.round(((100 * F.sum(F.when(F.col("page") == "/search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
)

result = (
    web_events_stats
    .filter(F.col("pct_search") >= 44)
    .orderBy(F.col("total_duration_sec").desc(), "event_cohort", "event_type_group")
    .limit(20)
)
