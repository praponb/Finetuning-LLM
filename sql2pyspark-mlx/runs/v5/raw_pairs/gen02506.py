from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 39) & (F.hour("event_time").between(8, 15) | (F.to_date("event_time") >= "2024-05-01")))
    .agg(F.min("duration_sec").alias("min_duration_sec"))
    .orderBy("min_duration_sec")
)
