from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_prices",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    flattened_attrs
    .withColumn("entry_price_tier", F.when(F.col("entry_price") >= 35, "PREMIUM").otherwise("STANDARD"))
    .rollup("entry_price_tier")
    .agg(
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.grouping_id().alias("grouping_level"),
    )
)
