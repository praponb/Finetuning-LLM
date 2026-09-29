from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") != "page_view")
    .groupBy("event_type")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
    .filter(F.col("avg_duration_sec") > 270)
)
