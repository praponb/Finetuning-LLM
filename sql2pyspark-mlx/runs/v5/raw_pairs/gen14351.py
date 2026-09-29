from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("plan").orderBy("avg_tier_price", "id")
w2 = Window.partitionBy("plan").orderBy("entry_price", "id")

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    unpacked
    .filter(F.col("avg_tier_price") < 21)
    .withColumn("prev_entry_price", F.lag("entry_price").over(w_grp))
    .withColumn("next_avg_tier_price", F.lead("avg_tier_price").over(w_grp))
    .withColumn("prev_avg_tier_price", F.lag("avg_tier_price").over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_grp))
    .withColumn("entry_price_bucket", F.ntile(3).over(w2))
)
