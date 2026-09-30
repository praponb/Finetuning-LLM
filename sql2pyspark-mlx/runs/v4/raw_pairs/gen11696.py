from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("entry_price", "id")
w2 = Window.partitionBy("plan").orderBy("entry_price", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("plan")

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "tier_limits",
        "tier_prices",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.avg("entry_price").over(w2).alias("entry_price_avg_6_prior"),
        F.avg("entry_price").over(w3).alias("plan_avg_entry_price"),
    )
)
