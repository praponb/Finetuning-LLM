from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type", F.date_format("event_time", "yyyy-MM").alias("event_month"))
    .agg(
        F.sum(F.when(F.col("event_type") == "search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
    .orderBy("search_duration_sec")
)
