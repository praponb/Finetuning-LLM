from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .groupBy("currency")
    .agg(
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.max(F.when(F.col("currency") == "THB", F.col("entry_price"))).alias("thb_max_entry_price"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows_2"),
    )
    .orderBy(F.col("usd_rows").asc_nulls_last())
)
