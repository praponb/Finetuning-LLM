from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("product_id").desc_nulls_last())
w2 = Window.partitionBy("code").orderBy(F.col("qty").desc())
w3 = Window.partitionBy("code").orderBy("qty")
w4 = Window.partitionBy("code").orderBy(F.col("qty_rank").desc_nulls_last())

exploded = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

exploded_carts = (
    exploded
    .select("id", "product_id", "qty", F.explode_outer("coupon_codes").alias("code"))
)

with_windows = (
    exploded_carts
    .filter(F.col("qty").isNotNull())
    .select(
        "id",
        "product_id",
        "qty",
        "code",
        F.dense_rank().over(w).alias("product_id_dense_rank"),
        F.rank().over(w2).alias("qty_rank"),
        F.dense_rank().over(w3).alias("qty_dense_rank"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "qty",
        "code",
        "product_id_dense_rank",
        F.rank().over(w4).alias("qty_rank_rank"),
    )
)

valid = (
    scored
    .filter(~F.col("code").eqNullSafe(F.lit("BUNDLE")))
    .withColumn("qty_minus_qty_rank_rank", (F.col("qty") - F.col("qty_rank_rank")))
    .withColumn("qty_rank_rank_filled", F.coalesce("qty_rank_rank", F.lit(0)))
)

result = (
    valid
    .select(
        "id",
        "qty",
        "qty_rank_rank",
        "qty_minus_qty_rank_rank",
        "qty_rank_rank_filled",
    )
)
