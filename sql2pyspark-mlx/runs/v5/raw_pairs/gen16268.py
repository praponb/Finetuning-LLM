from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.col("plan") == "starter")
    .select(
        "id",
        "plan",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("entry_price") >= 26, "PREMIUM").otherwise("STANDARD").alias("entry_price_tier"),
    )
    .agg(
        F.percentile_approx("entry_price", [0.25, 0.5, 0.75]).alias("entry_price_quartiles"),
        F.sum(F.when(F.col("plan") == "starter", F.col("lowest_price")).otherwise(0)).alias("starter_lowest_price"),
        F.max("lowest_price").alias("max_lowest_price"),
    )
    .orderBy(F.col("max_lowest_price").desc_nulls_last())
)
