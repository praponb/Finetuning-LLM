from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    unpacked
    .withColumn("avg_tier_price_tier", F.when(F.col("avg_tier_price") >= 32, "HIGH").otherwise("LOW"))
    .withColumn("lowest_price_tier", F.when(F.col("lowest_price") >= 31, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["avg_tier_price_tier", "currency", "lowest_price_tier"], ["avg_tier_price_tier", "currency"], ["avg_tier_price_tier"]],
        "avg_tier_price_tier",
        "currency",
        "lowest_price_tier",
    )
    .agg(
        F.sum(F.when(F.col("currency") == "EUR", F.col("avg_tier_price"))).alias("eur_total_avg_tier_price"),
        F.sum(F.when(F.col("currency") == "THB", F.col("avg_tier_price")).otherwise(0)).alias("thb_avg_tier_price"),
        F.round(F.sum("avg_tier_price") / F.nullif(F.count(F.when(F.col("currency") == "THB", 1)), F.lit(0)), 2).alias("avg_tier_price_per_thb"),
    )
    .orderBy("currency")
)
