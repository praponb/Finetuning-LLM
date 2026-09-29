from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 530, "high").when(F.col("duration_sec") >= 85, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.min("event_time").alias("first_event_time"),
    )
    .filter(F.col("avg_duration_sec") > 31)
    .select("duration_sec_bucket", "avg_duration_sec", "first_event_time")
    .orderBy(F.col("duration_sec_bucket").desc())
    .limit(50)
)
