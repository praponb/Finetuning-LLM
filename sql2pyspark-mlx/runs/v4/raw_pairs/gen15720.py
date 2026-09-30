from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("avg_tier_price", "id")
w_hist = Window.orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("effective_from", "id")
w4 = Window.orderBy("lowest_price", "id").rowsBetween(-6, Window.currentRow)

flattened_attrs = (
    df_price_tiers
    .filter(F.col("effective_from") < "2025-09-01")
    .select(
        "id",
        "effective_from",
        "tier_limits",
        "tier_prices",
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.lead("lowest_price", 2).over(w_ordered).alias("next_lowest_price"),
        F.sum("lowest_price").over(w_hist).alias("cumulative_lowest_price"),
        F.first("avg_tier_price", ignorenulls=True).over(w_hist).alias("first_known_avg_tier_price"),
        F.row_number().over(w3).alias("event_seq"),
        F.avg("lowest_price").over(w4).alias("lowest_price_avg_6"),
    )
)
