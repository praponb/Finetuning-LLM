from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("avg_tier_price", "id")
w2 = Window.orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan")

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    unpacked
    .filter(F.col("currency") != "EUR")
    .withColumn("avg_tier_price_bucket", F.ntile(4).over(w))
    .withColumn("running_lowest_price", F.sum("lowest_price").over(w2))
    .withColumn("plan_sum_avg_tier_price", F.sum("avg_tier_price").over(w3))
)
