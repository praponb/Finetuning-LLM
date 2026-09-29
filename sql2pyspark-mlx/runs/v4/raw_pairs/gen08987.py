from pyspark.sql import functions as F

kept_keys = (
    df_web_events
    .select("event_type")
    .subtract(
        df_web_events
        .filter(F.col("event_time") < "2024-01-01")
        .select("event_type")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("YEAR", "event_time").alias("year_start"))
    .agg(
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_rows"),
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_rows"),
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec"))).alias("search_total_duration_sec"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/search", 1)), F.lit(0)), 2).alias("duration_sec_per_search"),
        F.percentile_approx("duration_sec", 0.5).alias("duration_sec_p50"),
    )
)
