from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.year("effective_from") == 2025)
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .withColumn("activity_month", F.date_trunc("MONTH", "effective_from"))
    .rollup("activity_month")
    .agg(
        F.sum("entry_price").alias("total_entry_price"),
        F.avg("lowest_price").alias("avg_lowest_price"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("plan") == "starter", F.col("entry_price")).otherwise(0)).alias("starter_entry_price"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("activity_month").desc())
)
