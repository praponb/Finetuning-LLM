from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.col("currency").isin(["EUR", "THB"]))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "discounts",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .withColumn("entry_price_tier", F.when(F.col("entry_price") >= 26, "PREMIUM").otherwise("STANDARD"))
    .withColumn("year_start", F.date_trunc("YEAR", "effective_from"))
    .cube("entry_price_tier", "year_start")
    .agg(
        F.countDistinct("plan").alias("distinct_plans"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
        F.count("*").alias("row_count"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.round(F.sum("avg_tier_price") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("avg_tier_price_per_basic"),
        F.avg("avg_tier_price").alias("avg_avg_tier_price"),
        F.grouping("entry_price_tier").alias("gid"),
    )
    .orderBy(F.col("num_currencies").desc_nulls_last())
)
