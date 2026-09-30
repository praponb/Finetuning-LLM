from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") == 261)
    .groupBy("event_type")
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
    .filter(F.col("avg_duration_sec") > 120)
    .select("event_type", "first_event_time", "avg_duration_sec")
    .orderBy(F.col("avg_duration_sec").desc(), "event_type")
    .limit(3)
)
