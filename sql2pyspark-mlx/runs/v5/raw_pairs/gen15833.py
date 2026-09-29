from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.col("plan").isin(["pro", "basic"]))
    .select(
        "id",
        "plan",
        "currency",
        "tier_prices",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    parsed
    .groupBy("currency")
    .agg(
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.max("entry_price").alias("max_entry_price"),
        F.round(F.sum("lowest_price") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("lowest_price_per_usd"),
    )
    .orderBy(F.col("enterprise_events").asc_nulls_first())
)
