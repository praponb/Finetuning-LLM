from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        "plan",
        F.when(F.col("lowest_price") >= 30, "HIGH").otherwise("LOW").alias("lowest_price_tier"),
    )
    .agg(
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.percentile_approx("avg_tier_price", 0.95).alias("avg_tier_price_p95"),
        F.min("avg_tier_price").alias("min_avg_tier_price"),
    )
    .orderBy(F.col("avg_tier_price_p95").desc_nulls_last())
)
