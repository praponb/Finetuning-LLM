from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("avg_tier_price").desc_nulls_first())
w2 = Window.partitionBy("currency").orderBy("avg_tier_price", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("currency").orderBy("entry_price", "id")
w4 = Window.partitionBy("currency").orderBy("avg_tier_price", "id")

unpacked = (
    df_price_tiers
    .filter(~F.col("currency").isin(["EUR", "USD", "THB"]))
    .select(
        "id",
        "currency",
        "tier_limits",
        "tier_prices",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .withColumn("avg_tier_price_dense_rank", F.dense_rank().over(w))
    .withColumn("avg_tier_price_avg_6", F.avg("avg_tier_price").over(w2))
    .withColumn("next_entry_price", F.lead("entry_price", 1, 0).over(w3))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
)
