from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.col("plan").isin(["enterprise", "pro"]))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .withColumn("week_start", F.date_trunc("WEEK", "effective_from"))
    .withColumn("entry_price_tier", F.when(F.col("entry_price") >= 33, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["week_start", "plan", "entry_price_tier"], ["week_start", "plan"], []],
        "week_start",
        "plan",
        "entry_price_tier",
    )
    .agg(
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
        F.sum(F.when(F.col("currency") == "USD", F.col("entry_price")).otherwise(0)).alias("usd_entry_price"),
        F.count("*").alias("num_rows"),
        F.round(F.sum("entry_price") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("entry_price_per_basic"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_count"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
)
