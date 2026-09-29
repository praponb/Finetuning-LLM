from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "add_to_cart") & (F.col("duration_sec") == 171))
    .groupBy("event_type")
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
    .orderBy("event_type")
    .limit(10)
)
