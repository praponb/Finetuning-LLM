from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 11) & (F.col("page") == "/checkout"))
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        F.floor("duration_sec").alias("duration_sec_whole"),
    )
    .orderBy("event_type", "id")
    .limit(1)
)
