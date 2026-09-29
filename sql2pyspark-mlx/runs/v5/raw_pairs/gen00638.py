from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 487, "high").when(F.col("duration_sec") >= 183, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .orderBy("avg_duration_sec")
)
