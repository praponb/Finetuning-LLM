from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "event_time",
        "event_type",
        F.trim("event_type").alias("clean_event_type"),
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
    )
)
