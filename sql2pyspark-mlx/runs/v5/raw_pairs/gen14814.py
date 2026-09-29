from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "tier_prices",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    parsed
    .groupBy(F.date_trunc("YEAR", "effective_from").alias("year_start"))
    .agg(
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles"),
        F.percentile_approx("lowest_price", 0.95).alias("lowest_price_p95"),
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles_2"),
        F.percentile_approx("lowest_price", 0.95).alias("lowest_price_p95_2"),
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price"),
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price"),
    )
    .orderBy(F.col("avg_avg_tier_price").desc())
)
