from pyspark.sql import functions as F

valid_web_events = df_web_events.filter(F.col("page").isNull())

web_events_stats = (
    valid_web_events
    .groupBy("page", "event_type")
    .agg(
        F.count("*").alias("web_event_count"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

result = (
    web_events_stats
    .filter(F.col("web_event_count") >= 4)
    .select("page", "event_type", "web_event_count", "avg_duration_sec")
    .orderBy(F.col("web_event_count").desc(), "page", "event_type")
    .limit(5)
)
