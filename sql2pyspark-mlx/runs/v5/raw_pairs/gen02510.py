from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") >= "2025-09-05") & (F.col("duration_sec") == 159))
    .rollup(
        F.when(F.col("duration_sec") >= 510, "high").when(F.col("duration_sec") >= 95, "mid").otherwise("low").alias("duration_sec_bucket"),
        F.hour("event_time").alias("event_hour"),
    )
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.sum("duration_sec").alias("total_duration_sec"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
    .orderBy(F.col("first_event_time").desc())
)
