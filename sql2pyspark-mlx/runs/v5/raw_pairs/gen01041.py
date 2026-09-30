from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 1)
    .select(
        "id",
        "page",
        "event_type",
        F.lower("event_type").alias("event_type_lower"),
        F.when(F.col("duration_sec") >= 509, "High").when(F.col("duration_sec") >= 168, "Medium").otherwise("Low").alias("duration_sec_level"),
    )
)
