from pyspark.sql import functions as F

result = (
    df_price_tiers
    .filter(F.col("effective_from") <= "2024-09-01")
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_prices",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)
