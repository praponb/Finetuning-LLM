from pyspark.sql import functions as F

result = (
    df_web_events
    .agg(
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)
