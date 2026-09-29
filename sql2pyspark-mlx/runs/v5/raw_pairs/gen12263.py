from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("currency").orderBy("lowest_price", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("currency")
w4 = Window.partitionBy("currency").orderBy("entry_price", "id").rowsBetween(-3, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .withColumn("running_lowest_price", F.sum("lowest_price").over(w))
    .withColumn("entry_price_avg_2_prior", F.avg("entry_price").over(w2))
    .withColumn("currency_min_lowest_price", F.min("lowest_price").over(w3))
    .withColumn("lowest_price_avg_3", F.avg("lowest_price").over(w4))
)
