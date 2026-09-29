from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([3, 7, 19, 28]))
    .groupBy("page")
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.min("event_time").alias("min_event_time"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
    .orderBy("max_duration_sec", "min_event_time")
)
