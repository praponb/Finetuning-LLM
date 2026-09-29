from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("avg_tier_price").asc_nulls_first())
w2 = Window.partitionBy("currency").orderBy(F.col("entry_price").asc_nulls_last())

parsed = (
    df_price_tiers
    .filter(F.col("currency") != "USD")
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.cume_dist().over(w).alias("avg_tier_price_cume_dist"),
        F.percent_rank().over(w2).alias("entry_price_percent_rank"),
    )
)
