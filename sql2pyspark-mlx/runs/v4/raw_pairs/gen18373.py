from pyspark.sql import functions as F

filtered_web_events = df_web_events.filter(F.col("session_id").isin([17, 23, 29, 33]))

web_events_stats = (
    filtered_web_events
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
)

result = (
    web_events_stats
    .filter(F.col("total_duration_sec") > 1100)
    .orderBy("event_hour")
    .limit(10)
)
