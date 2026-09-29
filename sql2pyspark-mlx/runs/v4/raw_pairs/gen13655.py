from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") < "2025-06-01")
    .groupBy("page")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
        F.max("event_time").alias("latest_event_time"),
    )
    .filter(F.col("pct_search") >= 60)
    .select("page", "pct_search", "latest_event_time")
    .orderBy("pct_search", F.col("latest_event_time").desc(), "page")
    .limit(20)
)
