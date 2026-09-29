from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        "event_type",
        F.when(F.col("duration_sec") >= 532, "high").when(F.col("duration_sec") >= 181, "mid").otherwise("low").alias("duration_sec_bucket"),
        F.month("event_time").alias("event_month"),
    )
    .agg(
        F.countDistinct("duration_sec").alias("unique_event_types"),
        F.sum(F.when(F.col("page") == "/cart", F.col("duration_sec")).otherwise(0)).alias("cart_duration_sec"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)
