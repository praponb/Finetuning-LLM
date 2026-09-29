from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.min("event_time").alias("min_event_time"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0))) / F.count("*")), 1).alias("pct_click"),
    )
)
