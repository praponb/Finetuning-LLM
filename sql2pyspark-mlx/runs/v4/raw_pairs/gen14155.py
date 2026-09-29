from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan")
w_hist = Window.partitionBy("plan").orderBy("avg_tier_price", "id")
w_hist_2 = Window.partitionBy("plan").orderBy("lowest_price", "id")

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.stddev_samp("lowest_price").over(w_ordered).alias("plan_stddev_samp_lowest_price"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.ntile(4).over(w_hist_2).alias("lowest_price_bucket"),
    )
)
