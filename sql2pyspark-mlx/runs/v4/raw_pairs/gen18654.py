from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.col("currency") == "EUR")
    .select(
        "id",
        "currency",
        "discounts",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    parsed
    .groupBy(
        "currency",
        F.when(F.col("entry_price") >= 31, "PREMIUM").otherwise("STANDARD").alias("entry_price_tier"),
    )
    .agg(
        F.sum(F.when(F.col("currency") == "USD", F.col("entry_price")).otherwise(0)).alias("usd_entry_price"),
        F.countDistinct("currency").alias("distinct_currencies"),
    )
    .orderBy(F.col("entry_price_tier").desc())
)
