from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "tier_limits",
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        "plan",
        F.when(F.col("lowest_price") >= 36, "HIGH").otherwise("LOW").alias("lowest_price_tier"),
    )
    .agg(
        F.sum(F.when(F.col("plan") == "starter", F.col("entry_price")).otherwise(0)).alias("starter_entry_price"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("lowest_price")).otherwise(0)).alias("enterprise_lowest_price"),
        F.max("entry_price").alias("max_entry_price"),
    )
    .orderBy(F.col("starter_entry_price").desc())
)
