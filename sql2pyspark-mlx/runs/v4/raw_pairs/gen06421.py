from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("avg_tier_price", "id")
w2 = Window.partitionBy("plan", "currency")
w3 = Window.partitionBy("plan").orderBy(F.col("entry_price").desc())
w4 = Window.orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_price_tiers
    .filter(F.col("currency").isin(["EUR", "THB"]))
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.lag("avg_tier_price", 1).over(w).alias("prev_avg_tier_price"),
        F.min("avg_tier_price").over(w2).alias("plan_min_avg_tier_price"),
        F.row_number().over(w).alias("seq_no"),
        F.percent_rank().over(w3).alias("entry_price_percent_rank"),
        F.sum("entry_price").over(w4).alias("cumulative_entry_price"),
    )
)
