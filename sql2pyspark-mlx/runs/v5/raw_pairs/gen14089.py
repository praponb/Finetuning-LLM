from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    parsed
    .withColumn("quarter_start", F.date_trunc("QUARTER", "effective_from"))
    .withColumn("year_start", F.date_trunc("YEAR", "effective_from"))
    .cube("quarter_start", "year_start")
    .agg(
        F.countDistinct("currency").alias("distinct_currencies"),
        F.min("entry_price").alias("min_entry_price"),
        F.round(F.sum("avg_tier_price") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("avg_tier_price_per_usd"),
        F.grouping("quarter_start").alias("grouping_level"),
    )
    .orderBy(F.col("min_entry_price").desc_nulls_first())
)
