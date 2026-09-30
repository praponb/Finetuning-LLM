from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("lowest_price").desc_nulls_last())
w2 = Window.partitionBy("plan", "currency").orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan", "currency").orderBy("lowest_price", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("currency").orderBy(F.col("lowest_price").desc(), "id")
w5 = Window.partitionBy("currency", "plan")

flattened_attrs = (
    df_price_tiers
    .filter(F.col("currency").isin(["THB", "EUR"]))
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        "tier_prices",
        F.array_min("tier_prices").alias("lowest_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    flattened_attrs
    .withColumn("lowest_price_dense_rank", F.dense_rank().over(w))
    .withColumn("running_lowest_price", F.sum("lowest_price").over(w2))
    .withColumn("lowest_price_avg_5", F.avg("lowest_price").over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("currency_min_lowest_price", F.min("lowest_price").over(w5))
)
