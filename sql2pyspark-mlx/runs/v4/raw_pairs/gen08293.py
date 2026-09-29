from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("entry_price") >= 35, "LARGE").otherwise("SMALL").alias("entry_price_tier"),
    )
    .agg(
        F.percentile_approx("entry_price", [0.25, 0.5, 0.75]).alias("entry_price_quartiles"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("entry_price", 0.9).alias("entry_price_p90"),
        F.count("*").alias("num_rows_2"),
        F.count("*").alias("row_count"),
        F.count("*").alias("event_count"),
    )
)
