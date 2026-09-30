from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        "event_type",
        F.when(F.col("duration_sec") >= 511, "high").when(F.col("duration_sec") >= 156, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.max("event_time").alias("latest_event_time"))
    .orderBy(F.col("event_type").desc(), "duration_sec_bucket")
    .limit(3)
)
