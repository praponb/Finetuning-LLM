from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 455, "high").when(F.col("duration_sec") >= 136, "mid").otherwise("low").alias("duration_sec_bucket"),
        "session_id",
    )
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("latest_event_time")
)
