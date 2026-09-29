from pyspark.sql import functions as F

valid_web_events = (
    df_web_events
    .filter(F.col("session_id").isin([5, 6, 32, 39]) & F.col("session_id").isin([6, 39]))
)

summary = (
    valid_web_events
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
)

result = (
    summary
    .select("event_hour", "stddev_duration_sec")
    .orderBy("stddev_duration_sec")
)
