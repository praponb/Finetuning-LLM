from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 469, "HIGH").otherwise("LOW"))
    .rollup("event_type", "session_id", "duration_sec_tier")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("event_type") == "page_view", 1)), F.lit(0)), 2).alias("duration_sec_per_page_view"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("num_pages") >= 1)
)
