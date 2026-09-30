from pyspark.sql import functions as F

valid_web_events = (
    df_web_events
    .filter((F.col("event_time") < "2025-04-15") & ((F.col("event_type") == "add_to_cart") | (F.col("duration_sec") <= 232)))
    .withColumn("event_type_title", F.initcap(F.trim("event_type")))
)

agg = (
    valid_web_events
    .groupBy(F.date_format("event_time", "yyyy-MM").alias("event_month"))
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.max("event_time").alias("last_event_time"),
    )
)

result = (
    agg
    .filter(F.col("avg_duration_sec") >= 200)
    .select("event_month", "avg_duration_sec", "last_event_time")
)
