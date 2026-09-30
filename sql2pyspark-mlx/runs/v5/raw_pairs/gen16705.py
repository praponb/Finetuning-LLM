from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type", F.date_format("event_time", "yyyy-MM").alias("event_month"))
    .agg(F.countDistinct("duration_sec").alias("unique_event_types"))
    .filter(F.col("unique_event_types") < 10)
    .select("event_type", "event_month", "unique_event_types")
    .orderBy(F.col("unique_event_types").desc())
)
