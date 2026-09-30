from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    flattened_attrs
    .rollup(
        "currency",
        F.when(F.col("lowest_price") >= 30, "LARGE").otherwise("SMALL").alias("lowest_price_tier"),
    )
    .agg(
        F.percentile_approx("avg_tier_price", 0.5).alias("avg_tier_price_p50"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("eur_count").desc_nulls_last())
)
