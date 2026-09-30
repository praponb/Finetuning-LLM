from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") >= 267)
    .groupBy("page")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
)
