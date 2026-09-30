from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("lowest_price_tier").orderBy(F.col("avg_entry_price").desc())

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "tier_prices",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("lowest_price") >= 26, "HIGH").otherwise("LOW").alias("lowest_price_tier"),
        F.when(F.col("entry_price") >= 25, "HIGH").otherwise("LOW").alias("entry_price_tier"),
    )
    .agg(
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price"),
        F.percentile_approx("lowest_price", [0.25, 0.5, 0.75]).alias("lowest_price_quartiles"),
        F.percentile_approx("lowest_price", 0.95).alias("lowest_price_p95"),
    )
    .filter(F.col("lowest_price_p95") < 1000000)
    .withColumn("avg_entry_price_dense_rank", F.dense_rank().over(w))
    .orderBy("entry_price_tier")
)
