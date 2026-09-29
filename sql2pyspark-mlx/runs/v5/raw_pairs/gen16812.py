from pyspark.sql import functions as F

recent_web_events = df_web_events.filter(F.to_date("event_time") >= "2023-07-28")

agg = (
    recent_web_events
    .groupBy("event_type")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)

result = (
    agg
    .select("event_type", "min_duration_sec")
    .orderBy(F.col("min_duration_sec").desc())
)
