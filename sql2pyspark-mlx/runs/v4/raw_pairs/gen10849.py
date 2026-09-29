from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("currency").orderBy(F.col("lowest_price").asc_nulls_last())
w4 = Window.orderBy(F.col("lowest_price").desc_nulls_first())

unpacked = (
    df_price_tiers
    .filter(F.col("currency").isin(["USD", "THB", "EUR"]))
    .select(
        "id",
        "currency",
        "tier_limits",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    unpacked
    .withColumn("cumulative_lowest_price", F.sum("lowest_price").over(w))
    .withColumn("first_known_lowest_price", F.first("lowest_price", ignorenulls=True).over(w2))
    .withColumn("lowest_price_cume_dist", F.cume_dist().over(w3))
    .withColumn("lowest_price_percent_rank", F.percent_rank().over(w4))
)
