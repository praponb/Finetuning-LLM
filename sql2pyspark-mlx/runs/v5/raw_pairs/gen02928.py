from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("entry_price").desc(), "id")
w2 = Window.orderBy("entry_price", "id").rowsBetween(-2, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "tier_prices",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    unpacked
    .filter(F.col("avg_tier_price") <= 14)
    .select(
        "id",
        "plan",
        "currency",
        F.ntile(10).over(w_recent).alias("entry_price_bucket"),
        F.avg("avg_tier_price").over(w2).alias("avg_tier_price_avg_2"),
    )
)
