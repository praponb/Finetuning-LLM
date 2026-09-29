from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.orderBy(F.col("product_id_times_price").asc_nulls_first())
w3 = Window.orderBy(F.col("price").desc_nulls_first())

flattened = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

filtered = (
    flattened
    .filter((~F.col("promo").eqNullSafe(F.lit("SPRING10"))) & (F.col("price") <= 150))
    .withColumn("product_id_times_price", (F.col("product_id") * F.col("price")))
    .withColumn("promo_filled", F.coalesce("promo", F.lit("N/A")))
)

scored = (
    filtered
    .filter(F.col("product_id") <= 9)
    .select(
        "id",
        "qty",
        "price",
        "promo",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.stddev_samp("product_id").over(w).alias("promo_stddev_samp_product_id"),
        F.dense_rank().over(w2).alias("product_id_times_price_dense_rank"),
        F.dense_rank().over(w3).alias("price_dense_rank"),
    )
)

valid = (
    scored
    .filter(F.col("promo").isNotNull())
    .select(
        "id",
        "price",
        "promo",
        "promo_stddev_samp_product_id",
        "product_id_times_price_dense_rank",
        "price_dense_rank",
        (F.col("price_share") - F.col("qty")).alias("price_share_minus_qty"),
        F.coalesce("promo", F.lit("OTHER")).alias("promo_filled"),
    )
)

valid_carts = (
    valid
    .filter(F.col("promo").isNotNull())
    .withColumn("promo_length", F.length("promo"))
    .withColumn("promo_clean", F.coalesce("promo", F.lit("NONE")))
)

result = (
    valid_carts
    .select(
        "id",
        "price",
        "promo",
        "product_id_times_price_dense_rank",
        "promo_filled",
        "promo_clean",
    )
)
