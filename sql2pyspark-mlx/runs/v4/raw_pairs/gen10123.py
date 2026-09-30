from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.date_format("event_time", "yyyy-MM").alias("event_month"),
        "event_type",
        "page",
    )
    .agg(F.min("event_time").alias("first_event_time"))
)
