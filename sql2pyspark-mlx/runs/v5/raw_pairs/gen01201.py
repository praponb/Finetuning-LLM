from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.col("plan") == "pro")
    .select(
        "id",
        "plan",
        "currency",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("entry_price") >= 33, "HIGH").otherwise("LOW").alias("entry_price_tier"),
    )
    .agg(
        F.percentile_approx("entry_price", 0.9).alias("entry_price_p90"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.percentile_approx("entry_price", 0.9).alias("entry_price_p90_2"),
    )
    .orderBy(F.col("entry_price_p90_2").desc())
)
