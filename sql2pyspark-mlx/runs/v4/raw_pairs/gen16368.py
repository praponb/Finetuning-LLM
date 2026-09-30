from pyspark.sql import functions as F

result = (
    df_web_events
    .agg(
        F.min("event_time").alias("min_event_time"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
    .orderBy(F.col("duration_sec_range").desc())
)
