from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") < "2025-06-05")
    .select(
        "id",
        "page",
        "event_type",
        "duration_sec",
        F.col("duration_sec").cast("string").alias("duration_sec_str"),
        F.weekofyear("event_time").alias("event_time_weekofyear"),
    )
)
