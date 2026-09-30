from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "purchase")
    .groupBy(F.month("event_time").alias("event_month"))
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .filter(F.col("total_duration_sec") > 900)
)
