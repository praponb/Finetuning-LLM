from pyspark.sql import functions as F

result = (
    df_web_events
    .rollup("page", "event_type")
    .agg(F.min("event_time").alias("first_event_time"))
)
