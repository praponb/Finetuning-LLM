from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 459, "high").when(F.col("duration_sec") >= 212, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
    .filter(F.col("avg_duration_sec") < 59)
    .orderBy("avg_duration_sec", F.col("duration_sec_bucket").desc())
)
