from pyspark.sql import functions as F

valid_web_events = (
    df_web_events
    .filter(F.col("session_id").isin([4, 27]) & ((F.col("duration_sec") < 335) | (F.col("duration_sec") != 445)))
)

summary = (
    valid_web_events
    .groupBy("page", "session_id")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.max("event_time").alias("last_event_time"),
    )
)

result = (
    summary
    .select("page", "session_id", "max_duration_sec", "last_event_time")
    .orderBy("last_event_time", "page", "session_id")
    .limit(1)
)
