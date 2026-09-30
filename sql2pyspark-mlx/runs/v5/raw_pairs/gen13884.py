from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("lowest_price") >= 28, "PREMIUM").otherwise("STANDARD").alias("lowest_price_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.min("lowest_price").alias("min_lowest_price"),
        F.count("*").alias("cnt"),
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles"),
        F.percentile_approx("avg_tier_price", 0.5).alias("avg_tier_price_p50"),
    )
)
