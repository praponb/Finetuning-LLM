from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("currency").orderBy("avg_tier_price", "id").rowsBetween(-3, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_prices",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .filter(F.col("currency") == "THB")
    .select(
        "id",
        "currency",
        "avg_tier_price",
        F.count("avg_tier_price").over(w_recent).alias("last4_count_avg_tier_price"),
        F.max("avg_tier_price").over(w_recent).alias("last4_max_avg_tier_price"),
        F.min("avg_tier_price").over(w_recent).alias("last4_min_avg_tier_price"),
    )
)
