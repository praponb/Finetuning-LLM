from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("entry_price") >= 25, "LARGE").otherwise("SMALL").alias("entry_price_tier"),
        "currency",
    )
    .agg(
        F.min("entry_price").alias("min_entry_price"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_rows"),
        F.percentile_approx("lowest_price", 0.5).alias("lowest_price_p50"),
        F.percentile_approx("lowest_price", [0.25, 0.5, 0.75]).alias("lowest_price_quartiles"),
    )
)
