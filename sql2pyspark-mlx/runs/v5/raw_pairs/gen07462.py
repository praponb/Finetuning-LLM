from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
    .filter(F.col("avg_duration_sec") > 250)
    .select("event_type", "avg_duration_sec")
    .orderBy("event_type")
    .limit(10)
)
