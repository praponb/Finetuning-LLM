from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("activity_month").orderBy(F.col("lowest_price_p95").desc())

parsed = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    parsed
    .groupBy(
        F.date_trunc("month", "effective_from").alias("activity_month"),
        "currency",
    )
    .agg(
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.percentile_approx("lowest_price", [0.25, 0.5, 0.75]).alias("lowest_price_quartiles"),
        F.percentile_approx("lowest_price", 0.95).alias("lowest_price_p95"),
    )
    .withColumn("lowest_price_p95_dense_rank", F.dense_rank().over(w))
    .orderBy("currency")
)
