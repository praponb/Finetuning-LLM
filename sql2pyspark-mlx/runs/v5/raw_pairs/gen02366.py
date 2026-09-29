from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") != "/product")
    .groupBy(
        F.when(F.col("duration_sec") >= 464, "high").when(F.col("duration_sec") >= 103, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .filter(F.col("stddev_duration_sec") < 40)
    .orderBy(F.col("total_duration_sec").desc())
)
