from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy(F.col("lowest_price").desc_nulls_first())
w2 = Window.partitionBy("plan")
w_grp = Window.partitionBy("plan").orderBy("lowest_price", "id")

flattened_attrs = (
    df_price_tiers
    .filter(F.col("plan") == "pro")
    .select(
        "id",
        "plan",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.rank().over(w_part).alias("lowest_price_rank"),
        F.sum("lowest_price").over(w2).alias("plan_sum_lowest_price"),
        F.ntile(4).over(w_grp).alias("lowest_price_bucket"),
        F.lead("lowest_price", 2).over(w_grp).alias("next_lowest_price"),
    )
)
