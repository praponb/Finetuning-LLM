from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("effective_from", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("entry_price", "id")

parsed = (
    df_price_tiers
    .filter(F.year("effective_from") >= 2023)
    .select(
        "id",
        "effective_from",
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    parsed
    .withColumn("avg_tier_price_avg_6", F.avg("avg_tier_price").over(w))
    .withColumn("running_entry_price", F.sum("entry_price").over(w2))
    .withColumn("prev_avg_tier_price", F.lag("avg_tier_price", 1).over(w3))
)
