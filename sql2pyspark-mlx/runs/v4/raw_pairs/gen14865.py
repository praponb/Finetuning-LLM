from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([8, 11, 38]) & (F.to_date("event_time") < "2025-02-01"))
    .groupBy("session_id", F.month("event_time").alias("event_month"))
    .agg(
        F.countDistinct("duration_sec").alias("unique_pages"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("click_count"),
    )
    .filter(F.col("stddev_duration_sec") > 44)
    .orderBy(F.col("session_id").desc(), F.col("click_count").desc())
)
