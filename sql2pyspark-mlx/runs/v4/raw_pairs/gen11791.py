from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/search", "/cart"]))
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.countDistinct("duration_sec").alias("unique_event_types"),
    )
)
