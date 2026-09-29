from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "tier_prices",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    flattened_attrs
    .rollup("plan")
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.max("entry_price").alias("max_entry_price"),
        F.round(F.sum("entry_price") / F.nullif(F.count(F.when(F.col("plan") == "starter", 1)), F.lit(0)), 2).alias("entry_price_per_starter"),
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
    )
    .orderBy("plan")
)
