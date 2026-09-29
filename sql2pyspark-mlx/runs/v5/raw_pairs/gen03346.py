from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .groupBy("event_type", "session_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
        F.min("event_time").alias("first_event_time"),
    )
    .orderBy(F.col("event_type").desc(), "duration_sec_count")
)
