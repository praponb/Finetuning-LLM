from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .groupBy(
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
        "currency",
    )
    .agg(
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.avg(F.when(F.col("currency") == "USD", F.col("avg_tier_price"))).alias("usd_avg_avg_tier_price"),
    )
)
