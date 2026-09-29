from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("lowest_price", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy(F.col("lowest_price").desc(), "id")

unpacked = (
    df_price_tiers
    .filter(F.col("effective_from").between("2023-04-01", "2025-03-29"))
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "lowest_price",
        F.avg("lowest_price").over(w).alias("lowest_price_avg_3"),
        F.ntile(4).over(w2).alias("lowest_price_bucket"),
    )
)
