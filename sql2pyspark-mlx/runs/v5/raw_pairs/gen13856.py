from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.col("currency").isin(["EUR", "THB", "USD"]))
    .select(
        "id",
        "plan",
        "currency",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("entry_price") >= 28, "PREMIUM").otherwise("STANDARD").alias("entry_price_tier"),
    )
    .agg(
        F.percentile_approx("entry_price", [0.25, 0.5, 0.75]).alias("entry_price_quartiles"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.min("entry_price").alias("min_entry_price"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 1)
)
