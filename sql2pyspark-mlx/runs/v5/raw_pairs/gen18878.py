from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("entry_price", "id")
w2 = Window.partitionBy("plan").orderBy(F.col("avg_tier_price").desc(), "id")

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "tier_limits",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("entry_price") <= 21)
    .select(
        "id",
        "plan",
        F.lag("entry_price").over(w).alias("prev_entry_price"),
        F.lag("avg_tier_price").over(w).alias("prev_avg_tier_price"),
        F.lead("avg_tier_price").over(w).alias("next_avg_tier_price"),
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
