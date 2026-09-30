from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(((F.col("event_type") == "add_to_cart") & (F.col("duration_sec") == 197)) | (F.col("event_time") < "2024-05-21"))
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.max("event_time").alias("last_event_time"),
    )
    .filter(F.col("avg_duration_sec") < 400)
    .orderBy(
        F.col("avg_duration_sec").desc(),
        F.col("last_event_time").desc(),
        "event_month",
    )
    .limit(20)
)
