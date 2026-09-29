from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2023-11-01") & (F.col("event_time") >= "2024-01-01"))
    .groupBy("event_type")
    .agg(
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
    .filter(F.col("home_count") < 5)
    .orderBy("stddev_duration_sec")
)
