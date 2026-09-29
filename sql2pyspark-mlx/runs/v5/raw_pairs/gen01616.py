from pyspark.sql import functions as F

result = (
    df_price_tiers
    .filter(F.col("plan").isin(["enterprise", "basic"]))
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)
