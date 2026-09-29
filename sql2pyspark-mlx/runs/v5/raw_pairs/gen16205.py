from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "event_time",
        "page",
        "duration_sec",
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
        F.substring("event_type", 1, 4).alias("event_type_prefix"),
    )
)
