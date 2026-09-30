from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .filter(F.col("effective_from").between("2023-08-01", "2023-09-01"))
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    unpacked
    .groupBy(
        "plan",
        F.when(F.col("lowest_price") >= 28, "PREMIUM").otherwise("STANDARD").alias("lowest_price_tier"),
    )
    .agg(
        F.percentile_approx("lowest_price", 0.5).alias("lowest_price_p50"),
        F.round(F.sum("lowest_price") / F.nullif(F.count(F.when(F.col("plan") == "starter", 1)), F.lit(0)), 2).alias("lowest_price_per_starter"),
        F.round(F.sum("lowest_price") / F.nullif(F.count(F.when(F.col("plan") == "pro", 1)), F.lit(0)), 2).alias("lowest_price_per_pro"),
    )
)
