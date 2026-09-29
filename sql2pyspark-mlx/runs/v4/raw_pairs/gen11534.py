from pyspark.sql import functions as F

filtered_web_events = df_web_events.filter(F.col("event_time") >= "2025-08-25")

agg = (
    filtered_web_events
    .groupBy("event_type")
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
)

result = (
    agg
    .select("event_type", "stddev_duration_sec")
    .orderBy("event_type")
)
