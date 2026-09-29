from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.col("plan") == "starter")
    .select(
        "id",
        "plan",
        "tier_limits",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("avg_tier_price") >= 32, "HIGH").otherwise("LOW").alias("avg_tier_price_tier"),
    )
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", F.col("avg_tier_price"))).alias("enterprise_total_avg_tier_price"),
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_count"),
    )
    .orderBy(F.col("starter_count").desc())
)
