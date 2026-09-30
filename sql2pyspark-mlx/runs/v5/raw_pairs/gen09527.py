from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_prices",
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("quarter", "effective_from").alias("quarter_start"))
    .agg(
        F.countDistinct("plan").alias("distinct_plans"),
        F.max("lowest_price").alias("max_lowest_price"),
        F.round(F.avg("lowest_price"), 2).alias("avg_lowest_price"),
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price"),
    )
)
