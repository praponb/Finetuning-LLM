from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.col("effective_from") >= "2024-11-01")
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("avg_tier_price") >= 30, "LARGE").otherwise("SMALL").alias("avg_tier_price_tier"),
    )
    .agg(
        F.avg("avg_tier_price").alias("avg_avg_tier_price"),
        F.count("*").alias("event_count"),
    )
)
