from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("avg_tier_price").desc(), "id")
w2 = Window.partitionBy("plan").orderBy("effective_from", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_prices",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    unpacked
    .withColumn("avg_tier_price_bucket", F.ntile(10).over(w))
    .withColumn("avg_tier_price_avg_6", F.avg("avg_tier_price").over(w2))
    .withColumn("last_known_avg_tier_price", F.last("avg_tier_price", ignorenulls=True).over(w3))
)
