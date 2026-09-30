from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .filter(F.col("plan") == "starter")
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .groupBy("plan", "currency")
    .agg(
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.avg(F.when(F.col("currency") == "THB", F.col("entry_price"))).alias("thb_avg_entry_price"),
        F.sum(F.when(F.col("currency") == "THB", F.col("entry_price")).otherwise(0)).alias("thb_entry_price"),
    )
    .orderBy(F.col("plan").asc_nulls_first())
)
