from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("price").asc_nulls_last())
w2 = Window.orderBy(F.col("product_id").desc_nulls_last())

lines = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select("id", "coupon_codes", F.col("line.product_id"), F.col("line.price"))
)

metrics = (
    lines
    .select(
        "*",
        F.dense_rank().over(w_grp).alias("price_dense_rank"),
        F.dense_rank().over(w2).alias("product_id_dense_rank"),
    )
)

base = (
    metrics
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "id",
        "coupon_codes",
        "product_id",
        "price",
        "product_id_dense_rank",
        F.round((F.col("price_dense_rank") / F.col("price")), 2).alias("price_dense_rank_per_price"),
        F.when(F.col("product_id_dense_rank") >= 8, "High").when(F.col("product_id_dense_rank") >= 3, "Medium").otherwise("Low").alias("product_id_dense_rank_band"),
    )
)

filtered = (
    base
    .filter(F.col("price") <= 200)
    .select(
        "id",
        "coupon_codes",
        "product_id",
        "product_id_dense_rank",
        "price_dense_rank_per_price",
        (F.col("price_dense_rank_per_price") * F.col("price")).alias("price_dense_rank_per_price_times_price"),
    )
)

result = (
    filtered
    .select("id", "product_id", "product_id_dense_rank")
    .orderBy(F.col("product_id_dense_rank").desc_nulls_last())
)
