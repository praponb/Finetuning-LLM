from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "tier_prices",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("entry_price") >= 33, "PREMIUM").otherwise("STANDARD").alias("entry_price_tier"),
    )
    .agg(
        F.min("avg_tier_price").alias("min_avg_tier_price"),
        F.percentile_approx("entry_price", 0.95).alias("entry_price_p95"),
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price"),
    )
    .orderBy(F.col("entry_price_p95").desc_nulls_last())
)
