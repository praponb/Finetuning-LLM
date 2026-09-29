from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        "session_id",
        F.when(F.col("duration_sec") >= 457, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
        F.date_trunc("MONTH", "event_time").alias("activity_month"),
    )
    .agg(
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_rows"),
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_count"),
        F.sum(F.when(F.col("event_type") == "search", F.col("duration_sec"))).alias("search_total_duration_sec"),
    )
    .orderBy("duration_sec_tier")
)
