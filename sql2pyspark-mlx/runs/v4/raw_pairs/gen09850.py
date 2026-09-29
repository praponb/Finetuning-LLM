from pyspark.sql import functions as F

base = (
    df_web_events
    .filter((F.col("page") != "/checkout") & (F.col("event_time") >= "2023-03-30"))
)

web_events_stats = (
    base
    .groupBy("event_type")
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.count("*").alias("num_web_events"),
    )
)

result = (
    web_events_stats
    .filter(F.col("num_web_events") > 9)
    .orderBy("first_event_time")
)
