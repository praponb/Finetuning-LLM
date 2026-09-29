from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") < 170)
    .groupBy(
        F.when(F.col("duration_sec") >= 451, "high").when(F.col("duration_sec") >= 71, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .filter(F.col("avg_duration_sec") > 54)
    .orderBy("duration_sec_bucket")
)
