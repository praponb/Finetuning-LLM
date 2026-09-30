from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 467, "HIGH").otherwise("LOW"))
    .withColumn("activity_month", F.date_trunc("month", "event_time"))
    .groupingSets(
        [["duration_sec_tier", "activity_month"], ["duration_sec_tier"], []],
        "duration_sec_tier",
        "activity_month",
    )
    .agg(
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.min("event_time").alias("started_at"),
        F.grouping_id().alias("grouping_id"),
    )
)
