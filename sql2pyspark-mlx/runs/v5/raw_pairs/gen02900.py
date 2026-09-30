from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("entry_price", "id")
w2 = Window.orderBy(F.col("avg_tier_price").desc(), "id")
w3 = Window.orderBy("avg_tier_price", "id")
w4 = Window.orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    parsed
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "avg_tier_price",
        F.ntile(5).over(w).alias("entry_price_bucket"),
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(4).over(w3).alias("avg_tier_price_bucket"),
        F.sum("avg_tier_price").over(w4).alias("running_avg_tier_price"),
        F.lag("entry_price", 1).over(w3).alias("prev_entry_price"),
    )
)
