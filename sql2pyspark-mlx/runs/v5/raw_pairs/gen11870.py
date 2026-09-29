from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        "event_type",
        F.when(F.col("duration_sec") >= 379, "high").when(F.col("duration_sec") >= 79, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
    .filter(F.col("avg_duration_sec") < 63)
)
