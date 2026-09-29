from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/home")
    .select(
        "duration_sec",
        "event_type",
        F.date_trunc("year", "event_time").alias("event_time_period"),
    )
)
