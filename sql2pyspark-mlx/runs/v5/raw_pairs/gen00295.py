from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("entry_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_price_tiers
    .filter(F.col("currency") == "USD")
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_prices",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    unpacked
    .filter(F.col("avg_tier_price") > 18)
    .select(
        "id",
        "effective_from",
        "currency",
        "entry_price",
        F.min("entry_price").over(w).alias("running_min_entry_price"),
        F.avg("entry_price").over(w).alias("running_avg_entry_price"),
        F.max("entry_price").over(w).alias("running_max_entry_price"),
        F.count("entry_price").over(w).alias("running_count_entry_price"),
    )
)
