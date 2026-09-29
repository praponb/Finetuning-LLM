from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([12, 15, 20, 39]))
    .groupBy(F.quarter("event_time").alias("event_quarter"))
    .agg(F.max("event_time").alias("last_event_time"))
    .orderBy("last_event_time")
)
