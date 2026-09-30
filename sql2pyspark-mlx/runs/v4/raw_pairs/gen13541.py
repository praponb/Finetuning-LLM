from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("promo").orderBy(F.col("price").desc())
w3 = Window.orderBy(F.col("price").desc())
w4 = Window.orderBy(F.col("coupon_codes_count").desc_nulls_first())

lines = (
    df_carts
    .withColumn("li_item", F.explode_outer("items"))
    .select("id", "coupon_codes", F.col("li_item.price"), F.col("li_item.promo"))
)

with_windows = (
    lines
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w)), 4))
    .withColumn("price_percent_rank", F.percent_rank().over(w2))
)

filtered = (
    with_windows
    .filter(F.col("promo").isNotNull() & (F.col("price_percent_rank") < 0.34))
    .select(
        "id",
        "coupon_codes",
        "price",
        "price_percent_rank",
        F.array_contains("coupon_codes", "WELCOME5").alias("is_welcome5"),
        F.size("coupon_codes").alias("coupon_codes_count"),
        F.coalesce("promo", F.lit("N/A")).alias("promo_clean"),
    )
)

with_windows_carts = (
    filtered
    .select(
        "*",
        F.rank().over(w3).alias("price_rank"),
        F.percent_rank().over(w4).alias("coupon_codes_count_percent_rank"),
    )
)

result = (
    with_windows_carts
    .select(
        "is_welcome5",
        "coupon_codes_count",
        "price_rank",
        "coupon_codes_count_percent_rank",
    )
    .orderBy(F.col("coupon_codes_count_percent_rank").desc())
)
