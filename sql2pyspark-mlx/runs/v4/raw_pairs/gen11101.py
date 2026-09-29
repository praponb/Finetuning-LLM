from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 530, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["page", "duration_sec_tier"], ["page"]],
        "page",
        "duration_sec_tier",
    )
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_rows"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/checkout", 1)), F.lit(0)), 2).alias("duration_sec_per_checkout"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.count_if(F.col("event_type") == "click").alias("click_events"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.grouping("page").alias("grouping_level"),
    )
    .orderBy(F.col("duration_sec_per_checkout").desc_nulls_first())
)
