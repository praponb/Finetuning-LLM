from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.col("effective_from") < "2025-07-24")
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .groupBy("plan")
    .agg(
        F.max(F.when(F.col("plan") == "pro", F.col("entry_price"))).alias("pro_max_entry_price"),
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price"),
        F.count_if(F.col("plan") == "starter").alias("starter_events"),
        F.sum(F.when(F.col("plan") == "starter", F.col("entry_price")).otherwise(0)).alias("starter_entry_price"),
        F.percentile_approx("entry_price", 0.95).alias("entry_price_p95"),
        F.round(F.sum("entry_price") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("entry_price_per_enterprise"),
    )
    .filter(F.col("starter_events") >= 1)
)
