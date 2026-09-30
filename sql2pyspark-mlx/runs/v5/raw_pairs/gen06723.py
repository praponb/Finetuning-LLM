from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    parsed
    .cube(
        F.when(F.col("entry_price") >= 32, "PREMIUM").otherwise("STANDARD").alias("entry_price_tier"),
    )
    .agg(
        F.max(F.when(F.col("currency") == "THB", F.col("entry_price"))).alias("thb_max_entry_price"),
        F.sum("entry_price").alias("total_entry_price"),
        F.percentile_approx("entry_price", 0.95).alias("entry_price_p95"),
        F.sum(F.when(F.col("plan") == "basic", F.col("entry_price")).otherwise(0)).alias("basic_entry_price"),
        F.max(F.when(F.col("currency") == "USD", F.col("entry_price"))).alias("usd_max_entry_price"),
    )
)
