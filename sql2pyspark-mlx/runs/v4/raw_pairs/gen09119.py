from pyspark.sql import functions as F

result = (
    df_price_tiers
    .filter(F.col("effective_from").between("2025-07-19", "2025-10-01"))
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)
