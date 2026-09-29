from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
    .filter(F.col("avg_duration_sec") >= 390)
    .select("event_hour", "avg_duration_sec", "duration_sec_count")
)
