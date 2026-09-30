from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.col("currency") == "EUR")
    .select(
        "id",
        "effective_from",
        "currency",
        "discounts",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .cube(F.date_trunc("WEEK", "effective_from").alias("week_start"))
    .agg(
        F.sum(F.when(F.col("currency") == "THB", F.col("entry_price"))).alias("thb_total_entry_price"),
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price"),
        F.max(F.when(F.col("currency") == "USD", F.col("entry_price"))).alias("usd_max_entry_price"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
    )
)
