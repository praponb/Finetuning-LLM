from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 465, "high").when(F.col("duration_sec") >= 206, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
    .filter(F.col("duration_sec_count") < 4)
    .drop("duration_sec_count")
    .orderBy(F.col("total_duration_sec").desc(), "duration_sec_bucket")
)
