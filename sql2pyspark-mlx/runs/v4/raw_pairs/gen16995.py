from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") != 335) & (F.col("duration_sec") <= 164) & F.col("duration_sec").isNull())
    .groupBy(F.to_date("event_time").alias("event_day"))
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
    )
)
