from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") <= 126)
    .groupBy(F.quarter("event_time").alias("event_quarter"), "event_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
    .filter(F.col("pct_search") >= 79)
)
