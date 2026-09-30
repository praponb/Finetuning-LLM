from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_format("event_time", "yyyy-MM").alias("event_month"), "event_type")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
    .filter(F.col("min_duration_sec") < 400)
    .orderBy("min_duration_sec", "event_month", "event_type")
    .limit(20)
)
