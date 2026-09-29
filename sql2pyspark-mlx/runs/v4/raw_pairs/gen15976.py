from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/home") & (F.to_date("event_time") >= "2024-10-01"))
    .groupBy("event_type")
    .agg(
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_count"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .filter(F.col("search_count") >= 3)
    .orderBy(F.col("event_type").desc())
)
