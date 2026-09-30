from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
    .filter(F.col("avg_duration_sec") > 270)
    .orderBy(F.col("avg_duration_sec").desc(), "event_hour")
    .limit(3)
)
