from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    flattened_attrs
    .cube(
        F.when(F.col("lowest_price") >= 26, "PREMIUM").otherwise("STANDARD").alias("lowest_price_tier"),
    )
    .agg(
        F.count_if(F.col("plan") == "pro").alias("pro_events"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price"),
    )
    .filter(F.col("pro_events") > 1)
    .orderBy(F.col("enterprise_events").asc_nulls_first())
)
