from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([2, 8, 10, 26]))
    .select(
        "id",
        "session_id",
        "page",
        F.substring("event_type", 1, 2).alias("event_type_prefix"),
        F.weekofyear("event_time").alias("event_time_weekofyear"),
    )
    .orderBy(F.col("event_time_weekofyear").desc())
)
