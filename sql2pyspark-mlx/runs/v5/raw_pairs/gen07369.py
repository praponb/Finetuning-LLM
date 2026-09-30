from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") < "2023-12-20")
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("page") == "/search", 1).otherwise(0)).alias("search_count"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
)
