from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.year("effective_from") >= 2025)
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    flattened_attrs
    .groupBy("currency", "plan")
    .agg(
        F.percentile_approx("avg_tier_price", 0.9).alias("avg_tier_price_p90"),
        F.count_if(F.col("plan") == "pro").alias("pro_events"),
        F.percentile_approx("lowest_price", 0.9).alias("lowest_price_p90"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
    .orderBy(F.col("lowest_price_p90").desc_nulls_first())
)
