from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "event_type",
        "session_id",
        F.initcap(F.trim("page")).alias("page_title"),
        F.date_trunc("year", "event_time").alias("event_time_period"),
    )
)
