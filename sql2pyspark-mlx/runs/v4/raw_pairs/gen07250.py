from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("entry_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("currency").orderBy("entry_price", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("currency").orderBy("entry_price", "id")

unpacked = (
    df_price_tiers
    .select(
        "id",
        "currency",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    unpacked
    .withColumn("running_avg_tier_price", F.sum("avg_tier_price").over(w))
    .withColumn("entry_price_avg_6", F.avg("entry_price").over(w2))
    .withColumn("next_entry_price", F.lead("entry_price", 2).over(w3))
)
