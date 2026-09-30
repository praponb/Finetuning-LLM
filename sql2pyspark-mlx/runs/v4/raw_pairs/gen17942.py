from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "tier_limits",
        "discounts",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("lowest_price") >= 27, "PREMIUM").otherwise("STANDARD").alias("lowest_price_tier"),
        F.when(F.col("avg_tier_price") >= 33, "LARGE").otherwise("SMALL").alias("avg_tier_price_tier"),
    )
    .agg(
        F.percentile_approx("lowest_price", 0.95).alias("lowest_price_p95"),
        F.percentile_approx("avg_tier_price", 0.9).alias("avg_tier_price_p90"),
        F.percentile_approx("lowest_price", [0.25, 0.5, 0.75]).alias("lowest_price_quartiles"),
        F.count("*").alias("cnt"),
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price"),
    )
    .orderBy(F.col("avg_tier_price_p90").desc())
)
