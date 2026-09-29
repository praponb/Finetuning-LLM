from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.to_date("event_time").alias("event_day"))
    .agg(
        F.countDistinct("duration_sec").alias("unique_sessions"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
    .filter(F.col("duration_sec_count") >= 7)
    .select("event_day", "unique_sessions", "duration_sec_count")
)
