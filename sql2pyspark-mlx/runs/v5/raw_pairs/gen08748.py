from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("lowest_price").desc(), "id")
w2 = Window.partitionBy("plan").orderBy("lowest_price", "id")
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("plan").orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "tier_prices",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "plan",
        "lowest_price",
        F.row_number().over(w).alias("seq_no"),
        F.lead("lowest_price", 2).over(w2).alias("next_lowest_price"),
        F.stddev_samp("lowest_price").over(w3).alias("plan_stddev_samp_lowest_price"),
        F.last("lowest_price", ignorenulls=True).over(w4).alias("last_known_lowest_price"),
    )
)
