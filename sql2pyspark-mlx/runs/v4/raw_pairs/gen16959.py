from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_limits",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .withColumn("avg_tier_price_tier", F.when(F.col("avg_tier_price") >= 34, "PREMIUM").otherwise("STANDARD"))
    .rollup("avg_tier_price_tier", "currency")
    .agg(
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_count"),
        F.percentile_approx("avg_tier_price", 0.95).alias("avg_tier_price_p95"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.grouping("avg_tier_price_tier").alias("gid"),
    )
    .orderBy(F.col("thb_count").asc_nulls_last())
)
