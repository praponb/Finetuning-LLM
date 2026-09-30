from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_trunc("QUARTER", "event_time").alias("quarter_start"), "session_id")
    .agg(
        F.count_if(F.col("event_type") == "search").alias("search_events"),
        F.max("event_time").alias("last_event_time"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.max("event_time").alias("last_event_time_2"),
    )
)
