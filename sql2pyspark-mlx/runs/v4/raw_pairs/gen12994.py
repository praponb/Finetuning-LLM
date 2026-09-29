from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "page",
        "event_time",
        "session_id",
        F.when(F.col("duration_sec") >= 381, "High").when(F.col("duration_sec") >= 214, "Medium").otherwise("Low").alias("duration_sec_band"),
        F.round((F.col("duration_sec") * 0.9), 2).alias("duration_sec_adjusted"),
    )
)
