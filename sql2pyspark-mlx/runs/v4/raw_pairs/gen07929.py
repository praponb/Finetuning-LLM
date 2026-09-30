from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("lowest_price", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.partitionBy("currency").orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("currency").orderBy(F.col("lowest_price").desc())

parsed = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_prices",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.avg("avg_tier_price").over(w).alias("avg_tier_price_avg_2"),
        F.sum("avg_tier_price").over(w_part).alias("running_avg_tier_price"),
        F.first("lowest_price", ignorenulls=True).over(w_part).alias("first_known_lowest_price"),
        F.rank().over(w3).alias("lowest_price_rank"),
    )
)
