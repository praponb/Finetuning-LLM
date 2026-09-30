from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .filter(F.col("currency").isin(["THB", "USD", "EUR"]))
    .select(
        "id",
        "plan",
        "currency",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    unpacked
    .groupBy(
        "currency",
        F.when(F.col("avg_tier_price") >= 26, "PREMIUM").otherwise("STANDARD").alias("avg_tier_price_tier"),
    )
    .agg(
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.percentile_approx("avg_tier_price", 0.95).alias("avg_tier_price_p95"),
        F.round(F.avg("lowest_price"), 2).alias("avg_lowest_price"),
        F.count_if(F.col("currency") == "THB").alias("thb_events"),
    )
    .filter(F.col("eur_count") < 1)
    .orderBy("avg_tier_price_tier")
)
