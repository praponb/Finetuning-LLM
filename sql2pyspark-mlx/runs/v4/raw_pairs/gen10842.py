from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.col("currency") == "EUR")
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    parsed
    .groupBy(
        "plan",
        F.when(F.col("entry_price") >= 30, "PREMIUM").otherwise("STANDARD").alias("entry_price_tier"),
    )
    .agg(
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
        F.count_if(F.col("plan") == "pro").alias("pro_events"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.round(F.sum("entry_price") / F.nullif(F.count(F.when(F.col("plan") == "starter", 1)), F.lit(0)), 2).alias("entry_price_per_starter"),
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price"),
    )
    .filter(F.col("pro_events") >= 1)
    .orderBy(F.col("entry_price_per_starter").desc_nulls_first())
)
