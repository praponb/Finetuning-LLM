from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter((F.col("event_time") < "2024-11-01") & (F.to_date("event_time") < "2025-06-01"))
)

web_events_stats = (
    filtered_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 504, "high").when(F.col("duration_sec") >= 236, "mid").otherwise("low").alias("duration_sec_bucket"),
        "event_type",
    )
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)

result = (
    web_events_stats
    .select("duration_sec_bucket", "event_type")
    .orderBy("event_type", "duration_sec_bucket")
    .limit(10)
)
