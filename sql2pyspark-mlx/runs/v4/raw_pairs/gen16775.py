from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy("avg_tier_price", "id")

flattened_attrs = (
    df_price_tiers
    .filter(F.col("currency") == "EUR")
    .select(
        "id",
        "currency",
        "tier_limits",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .withColumn("currency_max_lowest_price", F.max("lowest_price").over(w))
    .withColumn("prev_entry_price", F.lag("entry_price", 2).over(w2))
)
