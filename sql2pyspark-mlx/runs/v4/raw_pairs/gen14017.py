from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.countDistinct("duration_sec").alias("unique_pages"),
        F.max("event_time").alias("last_event_time"),
    )
    .orderBy("last_event_time")
)
