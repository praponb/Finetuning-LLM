from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 477, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
        "event_type",
    )
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_rows"),
        F.sum(F.when(F.col("event_type") == "search", F.col("duration_sec"))).alias("search_total_duration_sec"),
        F.percentile_approx("duration_sec", 0.95).alias("duration_sec_p95"),
        F.count("*").alias("num_rows"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
    )
    .orderBy(F.col("event_type").desc())
)
