from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("lowest_price_tier").orderBy(F.col("lowest_price_p95").desc())

unpacked = (
    df_price_tiers
    .filter(F.col("currency").isin(["THB", "USD", "EUR"]))
    .select(
        "id",
        "currency",
        "tier_prices",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("lowest_price") >= 34, "HIGH").otherwise("LOW").alias("lowest_price_tier"),
        "currency",
    )
    .agg(
        F.percentile_approx("lowest_price", [0.25, 0.5, 0.75]).alias("lowest_price_quartiles"),
        F.percentile_approx("lowest_price", 0.95).alias("lowest_price_p95"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
    )
    .withColumn("lowest_price_p95_percent_rank", F.percent_rank().over(w))
)
