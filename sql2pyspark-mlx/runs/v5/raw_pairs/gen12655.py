from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.col("currency") == "THB")
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    parsed
    .groupBy("currency", F.date_trunc("YEAR", "effective_from").alias("year_start"))
    .agg(
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.percentile_approx("avg_tier_price", 0.95).alias("avg_tier_price_p95"),
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles"),
        F.sum(F.when(F.col("currency") == "USD", F.col("entry_price")).otherwise(0)).alias("usd_entry_price"),
    )
)
