from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    unpacked
    .groupBy(F.date_trunc("YEAR", "effective_from").alias("year_start"))
    .agg(
        F.avg("entry_price").alias("avg_entry_price"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price_2"),
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
    )
    .orderBy(F.col("avg_entry_price_2").desc())
)
