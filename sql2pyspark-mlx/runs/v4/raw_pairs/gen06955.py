from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("event_type") == "page_view") & (F.col("event_time") < "2025-09-01"))
    .withColumn("duration_sec_whole", F.ceil("duration_sec"))
)

web_events_stats = (
    recent_web_events
    .groupBy("event_type")
    .agg(F.count("duration_sec").alias("duration_sec_count"))
)

result = (
    web_events_stats
    .filter(F.col("duration_sec_count") > 7)
    .select("event_type", "duration_sec_count")
    .orderBy(F.col("duration_sec_count").desc())
)
