from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("effective_from", "id")
w2 = Window.orderBy(F.col("lowest_price").desc())

unpacked = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "tier_limits",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "lowest_price",
        F.lag("lowest_price", 2).over(w).alias("prev_lowest_price"),
        F.rank().over(w2).alias("lowest_price_rank"),
    )
)
