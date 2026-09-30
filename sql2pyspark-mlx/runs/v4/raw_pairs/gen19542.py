from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .filter(F.col("currency").isin(["EUR", "USD", "THB"]))
    .select(
        "id",
        "currency",
        "tier_prices",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    unpacked
    .groupBy("currency")
    .agg(
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
    )
    .filter(F.col("thb_rows") < 1)
    .orderBy(F.col("usd_rows").desc_nulls_first())
)
