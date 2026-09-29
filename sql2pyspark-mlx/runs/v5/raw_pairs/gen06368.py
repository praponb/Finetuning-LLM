from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(((F.col("duration_sec") != 422) & (F.col("event_type") == "page_view")) | F.col("session_id").isin([10, 15, 34]))
    .select(
        "id",
        "event_time",
        "duration_sec",
        "page",
        F.date_trunc("month", "event_time").alias("event_time_period"),
        F.floor("duration_sec").alias("duration_sec_whole"),
    )
)
