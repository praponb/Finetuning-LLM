from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("lowest_price").desc(), "id")
w3 = Window.partitionBy("currency")

parsed = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    parsed
    .filter(F.col("lowest_price") > 26)
    .select(
        "*",
        F.sum("lowest_price").over(w).alias("cumulative_lowest_price"),
        F.ntile(3).over(w2).alias("lowest_price_bucket"),
        F.avg("lowest_price").over(w3).alias("currency_avg_lowest_price"),
        F.count("lowest_price").over(w3).alias("currency_count_lowest_price"),
    )
)
