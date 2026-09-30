from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "tier_prices",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    parsed
    .groupBy("currency")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("avg_tier_price", 0.95).alias("avg_tier_price_p95"),
    )
    .orderBy("avg_tier_price_p95")
)
