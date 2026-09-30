from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(341, 462))
    .agg(F.max("event_time").alias("latest_event_time"))
    .orderBy("latest_event_time")
)
