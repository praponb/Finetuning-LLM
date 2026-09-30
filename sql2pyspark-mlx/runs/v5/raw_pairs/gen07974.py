from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .filter(F.col("currency") == "EUR")
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    unpacked
    .cube(
        F.when(F.col("avg_tier_price") >= 25, "LARGE").otherwise("SMALL").alias("avg_tier_price_tier"),
    )
    .agg(
        F.round(F.sum("avg_tier_price") / F.nullif(F.count(F.when(F.col("currency") == "EUR", 1)), F.lit(0)), 2).alias("avg_tier_price_per_eur"),
        F.sum(F.when(F.col("plan") == "pro", F.col("avg_tier_price")).otherwise(0)).alias("pro_avg_tier_price"),
        F.round(F.avg("lowest_price"), 2).alias("avg_lowest_price"),
    )
)
