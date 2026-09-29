from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("lowest_price", "id")
w3 = Window.orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("avg_tier_price", "id")

unpacked = (
    df_price_tiers
    .filter(F.col("effective_from").between("2024-06-26", "2025-06-01"))
    .select(
        "id",
        "effective_from",
        "tier_limits",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "lowest_price",
        "avg_tier_price",
        F.sum("lowest_price").over(w).alias("cumulative_lowest_price"),
        F.first("lowest_price", ignorenulls=True).over(w).alias("first_known_lowest_price"),
        F.row_number().over(w2).alias("event_seq"),
        F.first("lowest_price", ignorenulls=True).over(w3).alias("first_known_lowest_price_2"),
        F.ntile(3).over(w4).alias("avg_tier_price_bucket"),
    )
)
