from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") >= "2023-06-01")
    .groupBy("page", F.month("event_time").alias("event_month"))
    .agg(
        F.max("event_time").alias("latest_event_time"),
        F.count("*").alias("num_web_events"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .orderBy(F.col("event_month").desc())
)
