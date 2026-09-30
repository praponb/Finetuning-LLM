from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 1) & (F.col("session_id").isin([14, 24]) | (F.col("duration_sec") >= 429)))
    .groupBy("event_type", F.month("event_time").alias("event_month"))
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_add_to_cart"),
    )
    .orderBy("pct_add_to_cart", "event_type", "event_month")
    .limit(5)
)
