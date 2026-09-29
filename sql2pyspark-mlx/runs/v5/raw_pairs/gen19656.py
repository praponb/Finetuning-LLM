from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("avg_tier_price", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("entry_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_price_tiers
    .filter(F.col("plan") == "pro")
    .select(
        "id",
        "plan",
        "currency",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "plan",
        "lowest_price",
        "avg_tier_price",
        F.min("lowest_price").over(w).alias("last4_min_lowest_price"),
        F.avg("lowest_price").over(w).alias("last4_avg_lowest_price"),
        F.count("lowest_price").over(w).alias("last4_count_lowest_price"),
        F.last("avg_tier_price", ignorenulls=True).over(w2).alias("last_known_avg_tier_price"),
    )
)
