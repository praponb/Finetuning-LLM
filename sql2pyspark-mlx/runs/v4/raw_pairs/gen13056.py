from pyspark.sql import functions as F

result = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "tier_prices",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)
