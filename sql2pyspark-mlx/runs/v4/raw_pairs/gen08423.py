from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") < "2025-09-01")
    .select(
        "session_id",
        "event_time",
        F.date_format("event_time", "yyyy-MM-dd").alias("event_time_label"),
        F.date_trunc("WEEK", "event_time").alias("event_time_period"),
    )
)
