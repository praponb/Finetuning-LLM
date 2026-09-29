from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") == 378)
    .groupBy("event_type", F.date_format("event_time", "yyyy-MM").alias("event_month"))
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .filter(F.col("total_duration_sec") > 2100)
    .drop("total_duration_sec")
    .orderBy(F.col("avg_duration_sec").desc())
)
