from pyspark.sql import functions as F

valid_web_events = df_web_events.filter(F.to_date("event_time") < "2023-03-01")

web_events_stats = (
    valid_web_events
    .groupBy("page")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_count"),
    )
)

result = (
    web_events_stats
    .orderBy(F.col("click_count").desc(), "page")
    .limit(10)
)
