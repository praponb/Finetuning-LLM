from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.sum(F.when(F.col("event_type") == "page_view", F.col("duration_sec")).otherwise(0)).alias("page_view_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)
