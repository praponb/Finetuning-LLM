from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["purchase", "search"]))
    .select(
        "id",
        "duration_sec",
        "session_id",
        F.hour("event_time").alias("event_time_hour"),
        F.substring("event_type", 1, 2).alias("event_type_prefix"),
    )
)
