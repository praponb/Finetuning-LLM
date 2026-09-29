from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("avg_tier_price") >= 34, "LARGE").otherwise("SMALL").alias("avg_tier_price_tier"),
    )
    .agg(
        F.sum("avg_tier_price").alias("total_avg_tier_price"),
        F.min("lowest_price").alias("min_lowest_price"),
        F.count("*").alias("row_count"),
    )
)
