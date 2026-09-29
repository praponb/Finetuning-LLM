from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("entry_price", "id")
w2 = Window.orderBy("entry_price")
w3 = Window.orderBy(F.col("lowest_price").asc_nulls_last())
w4 = Window.orderBy("entry_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    unpacked
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("entry_price_percent_rank", F.percent_rank().over(w2))
    .withColumn("lowest_price_cume_dist", F.cume_dist().over(w3))
    .withColumn("last_known_avg_tier_price", F.last("avg_tier_price", ignorenulls=True).over(w4))
)
