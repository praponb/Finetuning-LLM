from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") < 429) | (F.col("duration_sec") > 179))
    .agg(F.max("event_time").alias("latest_event_time"))
)
