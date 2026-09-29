from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("lowest_price", "id")
w2 = Window.partitionBy("currency").orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("currency").orderBy("lowest_price", "id").rowsBetween(-6, -1)

flattened_attrs = (
    df_price_tiers
    .filter(F.col("currency").isin(["EUR", "THB", "USD"]))
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_prices",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .filter(~F.col("currency").isin(["USD", "THB"]))
    .select(
        "id",
        "avg_tier_price",
        "lowest_price",
        F.row_number().over(w).alias("row_seq"),
        F.sum("lowest_price").over(w2).alias("cumulative_lowest_price"),
        F.avg("lowest_price").over(w3).alias("lowest_price_avg_6_prior"),
        F.sum("avg_tier_price").over(w2).alias("running_avg_tier_price"),
    )
)
