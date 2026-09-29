from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency").orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("currency").orderBy(F.col("lowest_price").desc(), "id")
w3 = Window.orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("currency").orderBy("avg_tier_price", "id")

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.sum("avg_tier_price").over(w).alias("cumulative_avg_tier_price"),
        F.row_number().over(w2).alias("event_seq"),
        F.last("avg_tier_price", ignorenulls=True).over(w3).alias("last_known_avg_tier_price"),
        F.row_number().over(w4).alias("row_seq"),
    )
)
