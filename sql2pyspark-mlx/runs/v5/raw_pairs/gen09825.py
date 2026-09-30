from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_limits",
        "discounts",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    unpacked
    .groupBy("currency")
    .agg(
        F.sum(F.when(F.col("currency") == "EUR", F.col("entry_price")).otherwise(0)).alias("eur_entry_price"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
    )
    .orderBy("eur_entry_price")
)
