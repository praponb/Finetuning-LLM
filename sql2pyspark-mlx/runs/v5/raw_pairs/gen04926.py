from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(10, 15))
    .groupBy("event_type")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
    )
    .filter(F.col("home_count") >= 2)
    .orderBy("event_type")
)
