from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("currency").orderBy("lowest_price", "id")
w3 = Window.partitionBy("currency").orderBy(F.col("entry_price").desc_nulls_last())

parsed = (
    df_price_tiers
    .filter(F.col("currency").isin(["EUR", "USD", "THB"]))
    .select(
        "id",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    parsed
    .withColumn("running_entry_price", F.sum("entry_price").over(w))
    .withColumn("lowest_price_bucket", F.ntile(10).over(w2))
    .withColumn("entry_price_rank", F.rank().over(w3))
)
