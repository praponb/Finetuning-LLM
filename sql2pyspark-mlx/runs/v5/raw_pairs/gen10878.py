from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("session_id").isin([19, 26, 30, 36]) & F.col("session_id").isin([20, 30, 35]))
)

web_events_stats = (
    base
    .groupBy("page", F.hour("event_time").alias("event_hour"))
    .agg(F.count("*").alias("row_count"))
)

result = (
    web_events_stats
    .filter(F.col("row_count") >= 9)
    .select("page", "event_hour", "row_count")
)
