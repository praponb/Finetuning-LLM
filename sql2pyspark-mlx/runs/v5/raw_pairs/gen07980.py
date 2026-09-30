from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        "tier_prices",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    parsed
    .select(
        "id",
        "plan",
        F.sum("lowest_price").over(w).alias("running_sum_lowest_price"),
        F.count("lowest_price").over(w).alias("running_count_lowest_price"),
        F.min("lowest_price").over(w).alias("running_min_lowest_price"),
        F.avg("lowest_price").over(w).alias("running_avg_lowest_price"),
    )
)
