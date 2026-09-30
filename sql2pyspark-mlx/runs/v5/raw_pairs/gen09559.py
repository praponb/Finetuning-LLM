from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("plan").orderBy("lowest_price", "id")
w2 = Window.partitionBy("plan").orderBy(F.col("lowest_price").asc_nulls_first())
w3 = Window.partitionBy("plan").orderBy("avg_tier_price")
w4 = Window.partitionBy("plan")

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "tier_limits",
        "tier_prices",
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "plan",
        "lowest_price",
        F.lead("lowest_price", 1, 0).over(w_running).alias("next_lowest_price"),
        F.lag("lowest_price", 1).over(w_running).alias("prev_lowest_price"),
        F.dense_rank().over(w2).alias("lowest_price_dense_rank"),
        F.dense_rank().over(w3).alias("avg_tier_price_dense_rank"),
        F.count("avg_tier_price").over(w4).alias("plan_count_avg_tier_price"),
    )
)
