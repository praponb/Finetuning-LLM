from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(178, 466))
    .groupBy(
        "event_type",
        F.when(F.col("duration_sec") >= 401, "high").when(F.col("duration_sec") >= 201, "mid").otherwise("low").alias("duration_sec_bucket"),
        "page",
    )
    .agg(
        F.min("event_time").alias("min_event_time"),
        F.max("event_time").alias("last_event_time"),
        F.countDistinct("duration_sec").alias("unique_pages"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("duration_sec")).otherwise(0)).alias("add_to_cart_duration_sec"),
    )
    .filter(F.col("add_to_cart_duration_sec") > 37)
    .drop("add_to_cart_duration_sec")
    .orderBy(F.col("unique_pages").desc(), "event_type", "duration_sec_bucket", "page")
    .limit(50)
)
