from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").isNotNull())
    .select(
        "event_type",
        "event_time",
        "duration_sec",
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
    )
)
