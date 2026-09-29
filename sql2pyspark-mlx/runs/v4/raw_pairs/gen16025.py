from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2024-01-01") & F.hour("event_time").between(7, 15) & ~F.col("page").isin(["/product", "/checkout"]))
    .groupBy("event_type", "page")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.min("event_time").alias("first_event_time"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("first_event_time")
)
