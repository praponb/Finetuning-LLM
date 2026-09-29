from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_prices",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("entry_price") >= 26, "LARGE").otherwise("SMALL").alias("entry_price_tier"),
    )
    .agg(
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price"),
        F.avg(F.when(F.col("currency") == "USD", F.col("entry_price"))).alias("usd_avg_entry_price"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.round(F.sum("avg_tier_price") / F.nullif(F.count(F.when(F.col("currency") == "EUR", 1)), F.lit(0)), 2).alias("avg_tier_price_per_eur"),
        F.sum(F.when(F.col("currency") == "THB", F.col("avg_tier_price"))).alias("thb_total_avg_tier_price"),
    )
)
