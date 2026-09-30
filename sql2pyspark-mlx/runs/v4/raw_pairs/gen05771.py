from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") < "2025-10-01")
    .groupBy("event_type")
    .agg(
        F.countDistinct("duration_sec").alias("unique_sessions"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.min("event_time").alias("first_event_time"),
    )
)
