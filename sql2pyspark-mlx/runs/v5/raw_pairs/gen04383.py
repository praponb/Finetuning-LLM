from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("plan").orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy("entry_price", "id")

parsed = (
    df_price_tiers
    .filter(F.col("effective_from") <= "2024-01-07")
    .select(
        "id",
        "plan",
        "effective_from",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    parsed
    .select(
        "id",
        "entry_price",
        F.sum("entry_price").over(w_grp).alias("running_sum_entry_price"),
        F.avg("entry_price").over(w_grp).alias("running_avg_entry_price"),
        F.min("entry_price").over(w_grp).alias("running_min_entry_price"),
        F.max("entry_price").over(w_grp).alias("running_max_entry_price"),
        F.lag("avg_tier_price", 1).over(w2).alias("prev_avg_tier_price"),
    )
)
