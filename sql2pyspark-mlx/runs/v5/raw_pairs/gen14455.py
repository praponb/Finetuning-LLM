from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.year("event_time").alias("event_year"),
        F.when(F.col("duration_sec") >= 446, "high").when(F.col("duration_sec") >= 235, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.countDistinct("duration_sec").alias("unique_sessions"))
    .filter(F.col("unique_sessions") < 4)
)
