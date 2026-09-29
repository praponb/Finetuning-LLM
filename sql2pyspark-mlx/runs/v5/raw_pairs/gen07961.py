from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.col("currency") != "THB")
    .select(
        "id",
        "currency",
        "tier_limits",
        "tier_prices",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    parsed
    .groupBy("currency")
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("lowest_price", 0.5).alias("lowest_price_p50"),
        F.sum(F.when(F.col("currency") == "USD", F.col("entry_price")).otherwise(0)).alias("usd_entry_price"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
    )
    .orderBy(F.col("usd_events").asc_nulls_last())
)
