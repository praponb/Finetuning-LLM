from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("lowest_price") >= 27, "PREMIUM").otherwise("STANDARD").alias("lowest_price_tier"),
    )
    .agg(
        F.percentile_approx("lowest_price", 0.5).alias("lowest_price_p50"),
        F.round(F.avg("lowest_price"), 2).alias("avg_lowest_price"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("avg_lowest_price") > 0)
    .orderBy(F.col("lowest_price_tier").desc())
)
