from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/home") & F.col("event_type").isin(["purchase", "page_view"]))
    .groupBy(
        F.when(F.col("duration_sec") >= 463, "high").when(F.col("duration_sec") >= 85, "mid").otherwise("low").alias("duration_sec_bucket"),
        F.hour("event_time").alias("event_hour"),
    )
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
    .orderBy("event_hour", "duration_sec_bucket")
)
