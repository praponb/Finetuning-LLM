from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.col("effective_from") <= "2025-02-01")
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    flattened_attrs
    .cube(
        F.date_trunc("WEEK", "effective_from").alias("week_start"),
        F.date_trunc("YEAR", "effective_from").alias("year_start"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles"),
        F.avg(F.when(F.col("currency") == "EUR", F.col("avg_tier_price"))).alias("eur_avg_avg_tier_price"),
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price"),
        F.round(F.sum("avg_tier_price") / F.nullif(F.count(F.when(F.col("currency") == "THB", 1)), F.lit(0)), 2).alias("avg_tier_price_per_thb"),
    )
    .orderBy("year_start")
)
