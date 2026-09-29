from pyspark.sql import functions as F

result = (
    df_web_events
    .rollup(
        F.when(F.col("duration_sec") >= 532, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
        "page",
        F.date_trunc("WEEK", "event_time").alias("week_start"),
    )
    .agg(
        F.count_if(F.col("event_type") == "search").alias("search_events"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.max("event_time").alias("ended_at"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_rows"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.sum(F.when(F.col("page") == "/cart", F.col("duration_sec")).otherwise(0)).alias("cart_duration_sec"),
    )
)
