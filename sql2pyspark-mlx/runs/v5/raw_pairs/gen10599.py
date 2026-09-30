from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "tier_limits",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("lowest_price") >= 30, "LARGE").otherwise("SMALL").alias("lowest_price_tier"),
    )
    .agg(
        F.avg("lowest_price").alias("avg_lowest_price"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
