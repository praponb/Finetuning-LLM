from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .filter(F.col("effective_from") <= "2023-05-01")
    .select(
        "id",
        "effective_from",
        "tier_prices",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    unpacked
    .groupBy(F.date_trunc("quarter", "effective_from").alias("quarter_start"))
    .agg(
        F.percentile_approx("avg_tier_price", 0.5).alias("avg_tier_price_p50"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("avg_tier_price_p50") >= 0)
    .orderBy(F.col("avg_tier_price_p50").desc())
)
