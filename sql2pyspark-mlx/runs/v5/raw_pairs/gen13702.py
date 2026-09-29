from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("lowest_price").asc_nulls_last())
w2 = Window.partitionBy("plan").orderBy("lowest_price", "id")
w3 = Window.partitionBy("currency").orderBy(F.col("lowest_price").desc())

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    unpacked
    .filter(F.col("plan").isin(["enterprise", "starter", "pro"]))
    .select(
        "id",
        "plan",
        "currency",
        F.cume_dist().over(w).alias("lowest_price_cume_dist"),
        F.lag("lowest_price", 1).over(w2).alias("prev_lowest_price"),
        F.rank().over(w3).alias("lowest_price_rank"),
    )
)
