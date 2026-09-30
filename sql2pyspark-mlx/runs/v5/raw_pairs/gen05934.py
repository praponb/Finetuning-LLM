from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.min("event_time").alias("min_event_time"),
        F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0)).alias("search_count"),
    )
)
