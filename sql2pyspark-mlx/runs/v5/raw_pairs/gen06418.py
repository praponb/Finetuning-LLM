from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "tier_limits",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    parsed
    .withColumn("avg_tier_price_tier", F.when(F.col("avg_tier_price") >= 33, "HIGH").otherwise("LOW"))
    .rollup("avg_tier_price_tier")
    .agg(
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price"),
        F.count("*").alias("event_count"),
        F.min("avg_tier_price").alias("min_avg_tier_price"),
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("avg_tier_price_tier").desc_nulls_first())
)
