from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 380, "high").when(F.col("duration_sec") >= 127, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
    .filter(F.col("avg_duration_sec") > 340)
    .orderBy("avg_duration_sec")
)
