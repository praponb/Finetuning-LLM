from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("entry_price").desc_nulls_first())
w2 = Window.partitionBy("currency").orderBy("entry_price", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("currency").orderBy("avg_tier_price", "id")

flattened_attrs = (
    df_price_tiers
    .filter(F.col("effective_from") > "2025-05-22")
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .withColumn("entry_price_cume_dist", F.cume_dist().over(w))
    .withColumn("entry_price_avg_5", F.avg("entry_price").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)
