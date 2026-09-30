from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 10)
    .select(
        "id",
        "page",
        "event_type",
        F.dayofweek("event_time").alias("event_time_dayofweek"),
    )
)
