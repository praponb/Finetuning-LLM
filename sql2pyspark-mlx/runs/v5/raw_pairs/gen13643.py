from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency").orderBy("lowest_price", "id")
w2 = Window.orderBy("lowest_price", "id").rowsBetween(-6, -1)

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    parsed
    .select(
        "id",
        "plan",
        "lowest_price",
        F.lag("lowest_price").over(w).alias("prev_lowest_price"),
        F.lead("lowest_price").over(w).alias("next_lowest_price"),
        F.row_number().over(w).alias("seq_no"),
        F.avg("lowest_price").over(w2).alias("lowest_price_avg_6_prior"),
    )
)
