from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    parsed
    .groupBy(F.date_trunc("week", "effective_from").alias("week_start"))
    .agg(
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.count("*").alias("num_rows"),
        F.count_if(F.col("currency") == "THB").alias("thb_events"),
    )
    .orderBy(F.col("num_currencies").desc())
)
