from pyspark.sql import functions as F

valid_web_events = df_web_events.filter(F.col("duration_sec").between(215, 344))

web_events_stats = (
    valid_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 428, "high").when(F.col("duration_sec") >= 120, "mid").otherwise("low").alias("duration_sec_bucket"),
        "event_type",
    )
    .agg(
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
)

result = (
    web_events_stats
    .filter(F.col("stddev_duration_sec") > 73)
    .orderBy(F.col("duration_sec_bucket").desc(), "event_type")
    .limit(3)
)
