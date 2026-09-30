from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        "page",
        F.when(F.col("duration_sec") >= 470, "high").when(F.col("duration_sec") >= 221, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
    .orderBy("avg_duration_sec", "duration_sec_bucket", "page")
    .limit(20)
)
