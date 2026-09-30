from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("plan").orderBy("lowest_price", "id")
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy(F.col("lowest_price").desc(), "id")
w4 = Window.partitionBy("plan").orderBy("lowest_price")

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "tier_limits",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.ntile(3).over(w_running).alias("lowest_price_bucket"),
        F.max("lowest_price").over(w2).alias("plan_max_lowest_price"),
        F.lag("lowest_price", 1, 0).over(w_running).alias("prev_lowest_price"),
        F.row_number().over(w3).alias("event_seq"),
        F.rank().over(w4).alias("lowest_price_rank"),
    )
)
