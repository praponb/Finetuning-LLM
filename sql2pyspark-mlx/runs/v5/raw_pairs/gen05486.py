from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.col("currency").isin(["THB", "USD"]))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    flattened_attrs
    .cube(
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
        F.when(F.col("lowest_price") >= 26, "PREMIUM").otherwise("STANDARD").alias("lowest_price_tier"),
    )
    .agg(
        F.sum(F.when(F.col("currency") == "THB", F.col("entry_price")).otherwise(0)).alias("thb_entry_price"),
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price"),
    )
)
