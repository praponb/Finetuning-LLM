from pyspark.sql import functions as F

result = (
    df_price_tiers
    .filter(F.month("effective_from").isin([2, 8, 10]))
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)
