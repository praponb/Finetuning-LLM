from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([11, 17, 29, 30]))
    .select(
        "id",
        "event_time",
        F.date_trunc("month", "event_time").alias("event_time_period"),
    )
)
