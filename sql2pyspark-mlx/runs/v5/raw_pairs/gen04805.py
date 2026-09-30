from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(12, 18) & (F.col("event_type") == "search"))
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(F.min("duration_sec").alias("min_duration_sec"))
    .filter(F.col("min_duration_sec") >= 400)
)
