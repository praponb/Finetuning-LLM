from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_prices",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .groupBy("currency")
    .agg(
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_count"),
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("entry_price")).otherwise(0)).alias("enterprise_entry_price"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
    )
)
