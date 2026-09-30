from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") < "2024-12-01")
    .groupBy(F.quarter("event_time").alias("event_quarter"))
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .orderBy("min_duration_sec")
)
