from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .filter(F.col("currency") == "EUR")
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        "tier_prices",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("entry_price") >= 26, "PREMIUM").otherwise("STANDARD").alias("entry_price_tier"),
    )
    .agg(
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.percentile_approx("lowest_price", [0.25, 0.5, 0.75]).alias("lowest_price_quartiles"),
        F.percentile_approx("entry_price", 0.9).alias("entry_price_p90"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
    )
    .filter(F.col("usd_events") < 1)
    .orderBy(F.col("entry_price_tier").desc())
)
