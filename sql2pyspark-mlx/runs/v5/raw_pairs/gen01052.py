from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        "tier_prices",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("lowest_price") >= 36, "HIGH").otherwise("LOW").alias("lowest_price_tier"),
    )
    .agg(
        F.max("lowest_price").alias("max_lowest_price"),
        F.sum(F.when(F.col("plan") == "pro", F.col("lowest_price")).otherwise(0)).alias("pro_lowest_price"),
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price"),
        F.min("lowest_price").alias("min_lowest_price"),
    )
)
