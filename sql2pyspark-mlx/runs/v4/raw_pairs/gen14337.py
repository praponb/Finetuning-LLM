from pyspark.sql import functions as F

result = (
    df_price_tiers
    .filter(F.col("plan") != "basic")
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)
