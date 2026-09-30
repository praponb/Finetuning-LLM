from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") >= 448)
    .groupBy(F.month("event_time").alias("event_month"))
    .agg(F.min("duration_sec").alias("min_duration_sec"))
    .orderBy("event_month", F.col("min_duration_sec").desc())
    .limit(5)
)
