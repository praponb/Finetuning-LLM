from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("entry_price").desc(), "id")
w2 = Window.partitionBy("plan").orderBy(F.col("avg_tier_price").desc(), "id")

flattened_attrs = (
    df_price_tiers
    .filter(F.col("plan") == "enterprise")
    .select(
        "id",
        "plan",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("plan").isin(["starter", "pro"]))
    .select(
        "id",
        "avg_tier_price",
        "entry_price",
        F.ntile(10).over(w).alias("entry_price_bucket"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
