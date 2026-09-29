from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("avg_tier_price", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("avg_tier_price") < 17)
    .select(
        "id",
        "lowest_price",
        F.avg("lowest_price").over(w).alias("last3_avg_lowest_price"),
        F.sum("lowest_price").over(w).alias("last3_sum_lowest_price"),
        F.min("lowest_price").over(w).alias("last3_min_lowest_price"),
        F.first("avg_tier_price", ignorenulls=True).over(w2).alias("first_known_avg_tier_price"),
    )
)
