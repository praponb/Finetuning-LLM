from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        "tier_prices",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    unpacked
    .cube(
        F.date_trunc("QUARTER", "effective_from").alias("quarter_start"),
        F.date_trunc("WEEK", "effective_from").alias("week_start"),
        "plan",
    )
    .agg(
        F.percentile_approx("lowest_price", [0.25, 0.5, 0.75]).alias("lowest_price_quartiles"),
        F.percentile_approx("entry_price", 0.9).alias("entry_price_p90"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") < 1)
)
