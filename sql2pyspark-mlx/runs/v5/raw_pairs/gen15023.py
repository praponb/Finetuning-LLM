from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.hour("event_time").alias("event_hour"), "event_type")
    .agg(
        F.max("event_time").alias("latest_event_time"),
        F.countDistinct("duration_sec").alias("unique_sessions"),
    )
    .filter(F.col("unique_sessions") > 8)
    .select("event_hour", "event_type", "latest_event_time", "unique_sessions")
    .orderBy(F.col("latest_event_time").desc(), "event_hour", "event_type")
    .limit(5)
)
