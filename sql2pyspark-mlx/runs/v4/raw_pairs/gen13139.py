from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("duration_sec") >= 284)
    .withColumn("page_event_type_key", F.concat_ws(",", "page", "event_type"))
)

event_type_summary = (
    base
    .groupBy("event_type")
    .agg(
        F.max("event_time").alias("latest_event_time"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)

result = (
    event_type_summary
    .orderBy("event_type")
    .limit(5)
)
