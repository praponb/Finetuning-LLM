from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.quarter("event_time").alias("event_quarter"))
    .agg(
        F.countDistinct("duration_sec").alias("unique_sessions"),
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
    )
    .orderBy(F.col("unique_sessions").desc(), F.col("page_view_count").desc())
)
