from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .filter(F.col("effective_from") < "2024-02-01")
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    unpacked
    .groupBy(
        F.date_trunc("WEEK", "effective_from").alias("week_start"),
        F.when(F.col("entry_price") >= 34, "HIGH").otherwise("LOW").alias("entry_price_tier"),
    )
    .agg(
        F.max(F.when(F.col("currency") == "USD", F.col("entry_price"))).alias("usd_max_entry_price"),
        F.percentile_approx("entry_price", [0.25, 0.5, 0.75]).alias("entry_price_quartiles"),
    )
    .filter(F.col("usd_max_entry_price") >= 0)
    .orderBy(F.col("week_start").asc_nulls_first())
)
