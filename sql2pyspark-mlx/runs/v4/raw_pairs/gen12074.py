from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(420, 428))
    .groupBy(
        "event_type",
        F.when(F.col("duration_sec") >= 499, "high").when(F.col("duration_sec") >= 109, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.min("event_time").alias("first_event_time"),
    )
    .orderBy("avg_duration_sec")
)
