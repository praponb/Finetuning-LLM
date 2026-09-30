from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .cube(
        F.when(F.col("entry_price") >= 24, "HIGH").otherwise("LOW").alias("entry_price_tier"),
        "plan",
    )
    .agg(
        F.count_if(F.col("plan") == "starter").alias("starter_events"),
        F.round(F.sum("avg_tier_price") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("avg_tier_price_per_enterprise"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.sum(F.when(F.col("plan") == "basic", F.col("entry_price")).otherwise(0)).alias("basic_entry_price"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("avg_tier_price") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("avg_tier_price_per_enterprise_2"),
    )
    .filter(F.col("avg_tier_price_per_enterprise_2") < 0)
    .orderBy(F.col("plan").desc())
)
