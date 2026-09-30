from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.col("currency").isin(["THB", "EUR"]))
    .select(
        "id",
        "currency",
        "tier_limits",
        "tier_prices",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    flattened_attrs
    .withColumn("lowest_price_tier", F.when(F.col("lowest_price") >= 34, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["currency", "lowest_price_tier"], ["currency"], []],
        "currency",
        "lowest_price_tier",
    )
    .agg(
        F.sum("avg_tier_price").alias("total_avg_tier_price"),
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles"),
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price"),
        F.avg("lowest_price").alias("avg_lowest_price"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_count"),
    )
    .orderBy("avg_lowest_price")
)
