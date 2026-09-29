from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 403, "high").when(F.col("duration_sec") >= 231, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.min("event_time").alias("first_event_time"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
)
