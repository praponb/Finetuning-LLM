from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .filter(F.year("effective_from") == 2024)
    .select(
        "id",
        "effective_from",
        "tier_limits",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("lowest_price") >= 31, "PREMIUM").otherwise("STANDARD").alias("lowest_price_tier"),
    )
    .agg(
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles"),
        F.count("*").alias("row_count"),
        F.count("*").alias("event_count"),
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price"),
    )
    .filter(F.col("avg_avg_tier_price") > 0)
    .orderBy("lowest_price_tier")
)
