from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.min("event_time").alias("min_event_time"),
        F.countDistinct("duration_sec").alias("unique_pages"),
    )
    .filter(F.col("unique_pages") < 6)
    .select("page", "min_event_time", "unique_pages")
)
