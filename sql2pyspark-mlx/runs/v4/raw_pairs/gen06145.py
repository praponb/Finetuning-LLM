from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-10-21 19:00:00").cast("timestamp"))
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.col("duration_sec").cast("double").alias("duration_sec_num"),
    )
)

result = (
    base
    .groupBy("session_id")
    .pivot("event_type", ["add_to_cart", "search"])
    .agg(F.sum("duration_sec_num").alias("amount"))
)
