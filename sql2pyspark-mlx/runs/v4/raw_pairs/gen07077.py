from pyspark.sql import functions as F

result = (
    df_price_tiers
    .filter(F.col("effective_from").between("2024-11-01", "2025-06-01"))
    .select(
        "id",
        "effective_from",
        "tier_limits",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)
