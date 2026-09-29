from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("entry_price") >= 27, "PREMIUM").otherwise("STANDARD").alias("entry_price_tier"),
    )
    .agg(
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles"),
        F.percentile_approx("entry_price", [0.25, 0.5, 0.75]).alias("entry_price_quartiles"),
        F.percentile_approx("entry_price", [0.25, 0.5, 0.75]).alias("entry_price_quartiles_2"),
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price"),
    )
)
