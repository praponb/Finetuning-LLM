from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") == 367)
    .groupBy("event_type", "page")
    .agg(
        F.count("*").alias("num_web_events"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy(F.col("latest_event_time").desc())
)
