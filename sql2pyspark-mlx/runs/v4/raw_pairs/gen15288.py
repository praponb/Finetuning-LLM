from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .filter(F.col("plan").isin(["starter", "enterprise", "pro"]))
    .select(
        "id",
        "plan",
        "currency",
        "tier_prices",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .groupBy("plan")
    .agg(
        F.count_if(F.col("plan") == "pro").alias("pro_events"),
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.avg(F.when(F.col("plan") == "pro", F.col("entry_price"))).alias("pro_avg_entry_price"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
    )
    .filter(F.col("num_currencies") >= 1)
)
