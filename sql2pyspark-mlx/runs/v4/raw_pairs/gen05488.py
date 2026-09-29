from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("avg_tier_price", "id").rowsBetween(-5, Window.currentRow)
w_hist = Window.partitionBy("currency").orderBy("entry_price", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("plan", "currency").orderBy(F.col("entry_price").desc(), "id")

unpacked = (
    df_price_tiers
    .filter(F.year("effective_from") >= 2024)
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_prices",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .withColumn("avg_tier_price_avg_5", F.avg("avg_tier_price").over(w))
    .withColumn("lowest_price_avg_2", F.avg("lowest_price").over(w_hist))
    .withColumn("entry_price_bucket", F.ntile(5).over(w3))
)
