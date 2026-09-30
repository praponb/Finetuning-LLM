from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("avg_tier_price_tier").orderBy(F.col("avg_tier_price_p90").desc())

unpacked = (
    df_price_tiers
    .select(
        "id",
        "tier_prices",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("avg_tier_price") >= 27, "HIGH").otherwise("LOW").alias("avg_tier_price_tier"),
        F.when(F.col("lowest_price") >= 30, "PREMIUM").otherwise("STANDARD").alias("lowest_price_tier"),
    )
    .agg(
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price"),
        F.min("lowest_price").alias("min_lowest_price"),
        F.percentile_approx("avg_tier_price", 0.9).alias("avg_tier_price_p90"),
        F.max("lowest_price").alias("max_lowest_price"),
        F.percentile_approx("avg_tier_price", 0.9).alias("avg_tier_price_p90_2"),
        F.max("avg_tier_price").alias("max_avg_tier_price"),
    )
    .filter(F.col("min_lowest_price") >= 0)
    .withColumn("avg_tier_price_p90_percent_rank", F.percent_rank().over(w))
)
