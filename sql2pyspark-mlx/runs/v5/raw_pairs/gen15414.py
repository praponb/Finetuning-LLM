from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("entry_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("effective_from", "id")
w3 = Window.orderBy("entry_price", "id").rowsBetween(-2, -1)
w4 = Window.orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_price_tiers
    .filter(F.col("effective_from") <= "2025-08-01")
    .select(
        "id",
        "effective_from",
        "tier_limits",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("effective_from").between("2024-11-01", "2024-11-14"))
    .select(
        "id",
        "effective_from",
        "avg_tier_price",
        F.sum("entry_price").over(w).alias("cumulative_entry_price"),
        F.lead("avg_tier_price", 1).over(w2).alias("next_avg_tier_price"),
        F.avg("avg_tier_price").over(w3).alias("avg_tier_price_avg_2_prior"),
        F.first("avg_tier_price", ignorenulls=True).over(w4).alias("first_known_avg_tier_price"),
    )
)
