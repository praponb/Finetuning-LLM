from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("price").desc())
w2 = Window.orderBy(F.col("product_id_filled").asc_nulls_first())
w_running = Window.orderBy(F.col("qty").asc_nulls_last())

lines = (
    df_carts
    .withColumn("rec", F.explode("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("rec.product_id"),
        F.col("rec.qty"),
        F.col("rec.price"),
    )
)

valid = (
    lines
    .filter(F.size("coupon_codes") > 2)
    .select("*", F.coalesce("product_id", F.lit(0)).alias("product_id_filled"))
)

scored = (
    valid
    .select(
        "id",
        "coupon_codes",
        "qty",
        "price",
        "product_id_filled",
        F.cume_dist().over(w_grp).alias("price_cume_dist"),
        F.dense_rank().over(w2).alias("product_id_filled_dense_rank"),
        F.dense_rank().over(w_running).alias("qty_dense_rank"),
    )
)

filtered = (
    scored
    .filter(F.col("price_cume_dist").between(0.43, 0.59))
    .select(
        "*",
        F.when(F.col("price") >= 204, "High").when(F.col("price") >= 85, "Medium").otherwise("Low").alias("price_level"),
    )
)

result = (
    filtered
    .select("coupon_codes", "price", "price_cume_dist", "qty_dense_rank", "price_level")
)
