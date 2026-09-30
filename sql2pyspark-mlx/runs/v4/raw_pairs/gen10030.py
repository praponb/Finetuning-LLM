from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter(F.col("session_id") == 34)
    .withColumn("event_time_day", F.col("event_time").cast("date"))
)

event_type_summary = (
    filtered_web_events
    .groupBy("event_type", "page")
    .agg(
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_count"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)

result = event_type_summary.filter(F.col("search_count") < 5)
