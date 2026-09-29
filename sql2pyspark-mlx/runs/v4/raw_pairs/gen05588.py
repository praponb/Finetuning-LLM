from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("promo").orderBy(F.col("qty").desc_nulls_last())
w3 = Window.partitionBy("promo").orderBy(F.col("price_share_2").desc_nulls_first())

lines = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

with_windows = (
    lines
    .select(
        "*",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.count("price").over(w).alias("promo_count_price"),
    )
)

with_windows_carts = (
    with_windows
    .withColumn("qty_dense_rank", F.dense_rank().over(w2))
    .withColumn("price_share_2", F.round((F.col("price") / F.sum("price").over(w)), 4))
)

base = (
    with_windows_carts
    .filter((F.col("created_at") >= F.lit("2025-03-02").cast("date")) & F.col("promo").isNotNull() & F.col("qty_dense_rank").isNull())
    .withColumn("is_spring10", F.array_contains("coupon_codes", "SPRING10"))
)

result = (
    base
    .withColumn("price_share_2_rank", F.rank().over(w3))
    .withColumn("promo_count_price_share", F.count("price_share").over(w))
)
