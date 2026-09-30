from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 398, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
        F.date_trunc("YEAR", "event_time").alias("year_start"),
        "page",
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "page_view", F.col("duration_sec"))).alias("page_view_total_duration_sec"),
        F.percentile_approx("duration_sec", 0.95).alias("duration_sec_p95"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.sum("duration_sec").alias("total_duration_sec"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.count_if(F.col("event_type") == "click").alias("click_events"),
    )
    .filter(F.col("duration_sec_stddev") > 0)
)
