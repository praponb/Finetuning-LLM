from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("lowest_price") >= 34, "PREMIUM").otherwise("STANDARD").alias("lowest_price_tier"),
        "plan",
    )
    .agg(
        F.countDistinct("plan").alias("distinct_plans"),
        F.round(F.sum("lowest_price") / F.nullif(F.count(F.when(F.col("plan") == "starter", 1)), F.lit(0)), 2).alias("lowest_price_per_starter"),
    )
)
