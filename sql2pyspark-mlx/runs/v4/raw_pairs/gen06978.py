from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("qty").desc_nulls_last())
w2 = Window.partitionBy("promo").orderBy(F.col("qty").desc_nulls_first())
w3 = Window.partitionBy("promo")

lines = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    lines
    .select(
        "*",
        F.rank().over(w).alias("qty_rank"),
        F.dense_rank().over(w2).alias("qty_dense_rank"),
        F.round((F.col("price") / F.sum("price").over(w3)), 4).alias("price_share"),
    )
)
