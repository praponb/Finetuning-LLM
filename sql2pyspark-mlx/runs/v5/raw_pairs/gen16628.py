from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    parsed
    .groupBy("plan")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price"),
        F.round(F.sum("avg_tier_price") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("avg_tier_price_per_basic"),
    )
)
