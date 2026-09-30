from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") < 458)
    .select(
        "id",
        "session_id",
        F.col("event_time").cast("date").alias("event_time_day"),
        F.to_date("event_time").alias("event_time_date"),
    )
)
