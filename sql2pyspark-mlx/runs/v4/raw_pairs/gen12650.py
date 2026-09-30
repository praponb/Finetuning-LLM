from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan")
w2 = Window.partitionBy("plan")
w3 = Window.orderBy("lowest_price", "id")
w4 = Window.partitionBy("currency").orderBy("lowest_price", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("plan", "currency").orderBy(F.col("lowest_price").desc(), "id")

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        F.array_min("tier_prices").alias("lowest_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    flattened_attrs
    .withColumn("currency_max_lowest_price", F.max("lowest_price").over(w))
    .withColumn("plan_sum_lowest_price", F.sum("lowest_price").over(w2))
    .withColumn("prev_lowest_price", F.lag("lowest_price", 1).over(w3))
    .withColumn("lowest_price_avg_5_prior", F.avg("lowest_price").over(w4))
    .withColumn("lowest_price_bucket", F.ntile(5).over(w5))
)
