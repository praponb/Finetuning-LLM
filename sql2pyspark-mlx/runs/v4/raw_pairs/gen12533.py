from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.month("event_time").alias("event_month"))
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)
