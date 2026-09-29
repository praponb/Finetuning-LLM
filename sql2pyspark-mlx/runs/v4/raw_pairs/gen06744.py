from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") >= "2023-03-16") & ((F.col("duration_sec") != 200) | (F.to_date("event_time") < "2024-02-04")))
    .select(
        "event_time",
        "page",
        F.hour("event_time").alias("event_time_hour"),
        F.round((F.col("duration_sec") * 1.1), 2).alias("adjusted_duration_sec"),
    )
)
