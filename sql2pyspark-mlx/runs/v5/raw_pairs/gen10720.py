from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.col("plan") != "starter")
    .select(
        "id",
        "plan",
        "tier_prices",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .groupBy("plan")
    .agg(
        F.count_if(F.col("plan") == "starter").alias("starter_events"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("avg_tier_price")).otherwise(0)).alias("enterprise_avg_tier_price"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("starter_events") > 1)
    .orderBy(F.col("plan").desc_nulls_last())
)
