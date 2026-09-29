from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") != "/search") & (F.col("event_time") < "2025-05-21"))
    .select(
        "id",
        "duration_sec",
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
    )
)
