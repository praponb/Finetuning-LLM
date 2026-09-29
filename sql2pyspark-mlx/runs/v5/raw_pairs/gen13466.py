from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter((F.col("session_id") == 37) & (F.col("event_type") != "add_to_cart"))
)

web_events_stats = (
    filtered_web_events
    .groupBy(F.date_format("event_time", "yyyy-MM").alias("event_month"))
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)

result = web_events_stats.orderBy(F.col("min_duration_sec").desc(), "event_month")
