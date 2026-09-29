from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([4, 36]))
    .groupBy(F.quarter("event_time").alias("event_quarter"), "page")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.countDistinct("duration_sec").alias("unique_event_types"),
    )
)
