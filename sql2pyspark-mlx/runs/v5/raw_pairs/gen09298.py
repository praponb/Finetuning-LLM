from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "tier_limits",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    parsed
    .rollup(F.date_trunc("YEAR", "effective_from").alias("year_start"))
    .agg(
        F.percentile_approx("avg_tier_price", 0.95).alias("avg_tier_price_p95"),
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles"),
        F.percentile_approx("avg_tier_price", 0.95).alias("avg_tier_price_p95_2"),
        F.avg("avg_tier_price").alias("avg_avg_tier_price"),
    )
    .filter(F.col("avg_avg_tier_price") < 1000000)
    .orderBy(F.col("year_start").desc_nulls_last())
)
