from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([8, 14, 18, 26]))
    .select(
        "id",
        "event_type",
        "event_time",
        F.when(F.col("duration_sec") >= 424, "High").when(F.col("duration_sec") >= 78, "Medium").otherwise("Low").alias("duration_sec_level"),
    )
)
