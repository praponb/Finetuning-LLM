from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan").orderBy("avg_tier_price", "id")
w_recent = Window.partitionBy("plan").orderBy("entry_price", "id").rowsBetween(-3, -1)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    unpacked
    .filter(F.col("entry_price") < 11)
    .select(
        "id",
        "effective_from",
        "entry_price",
        F.lead("avg_tier_price").over(w_ordered).alias("next_avg_tier_price"),
        F.lag("avg_tier_price").over(w_ordered).alias("prev_avg_tier_price"),
        F.lead("entry_price").over(w_ordered).alias("next_entry_price"),
        F.lag("entry_price").over(w_ordered).alias("prev_entry_price"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.avg("avg_tier_price").over(w_recent).alias("avg_tier_price_avg_3_prior"),
    )
)
