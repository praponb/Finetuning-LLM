from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("entry_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy(F.col("lowest_price").desc())

flattened_attrs = (
    df_price_tiers
    .filter(F.col("effective_from") > "2023-11-01")
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .withColumn("cumulative_avg_tier_price", F.sum("avg_tier_price").over(w))
    .withColumn("plan_min_avg_tier_price", F.min("avg_tier_price").over(w2))
    .withColumn("plan_stddev_samp_lowest_price", F.stddev_samp("lowest_price").over(w2))
    .withColumn("lowest_price_percent_rank", F.percent_rank().over(w3))
)
