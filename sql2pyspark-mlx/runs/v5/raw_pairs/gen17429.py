from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "tier_prices",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("avg_tier_price") >= 36, "LARGE").otherwise("SMALL").alias("avg_tier_price_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("event_count"),
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("avg_tier_price", 0.95).alias("avg_tier_price_p95"),
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles"),
    )
    .orderBy(F.col("event_count").desc_nulls_first())
)
