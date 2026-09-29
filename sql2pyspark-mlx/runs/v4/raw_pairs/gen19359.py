from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.col("effective_from").between("2024-01-19", "2025-05-22"))
    .select(
        "id",
        "effective_from",
        "tier_prices",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("lowest_price") >= 26, "PREMIUM").otherwise("STANDARD").alias("lowest_price_tier"),
        F.when(F.col("avg_tier_price") >= 34, "PREMIUM").otherwise("STANDARD").alias("avg_tier_price_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price"),
        F.min("lowest_price").alias("min_lowest_price"),
    )
    .orderBy("avg_tier_price_tier")
)
