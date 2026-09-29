from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("avg_tier_price", "id").rowsBetween(-6, -1)

parsed = (
    df_price_tiers
    .select(
        "id",
        "tier_limits",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    parsed
    .withColumn("running_lowest_price", F.sum("lowest_price").over(w))
    .withColumn("avg_tier_price_avg_6_prior", F.avg("avg_tier_price").over(w2))
)
