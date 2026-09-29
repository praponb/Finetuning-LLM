from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") >= "2023-04-25")
    .groupBy("page")
    .agg(F.max("event_time").alias("last_event_time"))
)
