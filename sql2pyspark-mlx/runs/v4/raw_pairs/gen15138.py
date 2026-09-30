from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("lowest_price").desc(), "id")

parsed = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_limits",
        F.array_min("tier_prices").alias("lowest_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    parsed
    .withColumn("currency_stddev_samp_avg_tier_price", F.stddev_samp("avg_tier_price").over(w))
    .withColumn("lowest_price_bucket", F.ntile(10).over(w2))
    .withColumn("currency_avg_entry_price", F.avg("entry_price").over(w))
    .withColumn("currency_count_entry_price", F.count("entry_price").over(w))
)
