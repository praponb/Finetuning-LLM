from pyspark.sql import functions as F

result = (
    df_web_events
    .agg(F.min("event_time").alias("first_event_time"))
    .orderBy(F.col("first_event_time").desc())
)
