from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element").orderBy(F.col("qty").desc_nulls_first())
w2 = Window.partitionBy("customer_id").orderBy(F.col("product_id").desc_nulls_last())
w3 = Window.partitionBy("promo")

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "line"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

exploded = (
    flattened
    .select(
        "customer_id",
        "created_at",
        "product_id",
        "qty",
        "price",
        "promo",
        F.explode("coupon_codes").alias("element"),
    )
)

with_windows = (
    exploded
    .filter(F.col("qty") == 3)
    .withColumn("qty_percent_rank", F.percent_rank().over(w))
    .withColumn("product_id_dense_rank", F.dense_rank().over(w2))
)

scored = (
    with_windows
    .select(
        "created_at",
        "product_id",
        "qty_percent_rank",
        "product_id_dense_rank",
        F.count("price").over(w3).alias("promo_count_price"),
    )
)

valid = (
    scored
    .filter(F.col("qty_percent_rank") > 0.52)
    .select(
        "created_at",
        "product_id",
        "product_id_dense_rank",
        "promo_count_price",
        F.to_date("created_at").alias("created_at_date"),
    )
)

base = (
    valid
    .filter(F.col("created_at") >= "2024-02-15")
    .select(
        "*",
        F.round((F.col("promo_count_price") / F.col("product_id")), 2).alias("promo_count_price_per_product_id"),
    )
)

result = (
    base
    .rollup(
        F.when(F.col("product_id") >= 11, "PREMIUM").otherwise("STANDARD").alias("product_id_tier"),
        "product_id",
        F.when(F.col("promo_count_price_per_product_id") >= 89, "LARGE").otherwise("SMALL").alias("promo_count_price_per_product_id_tier"),
    )
    .agg(
        F.min("created_at").alias("first_created_at"),
        F.percentile_approx("promo_count_price", [0.25, 0.5, 0.75]).alias("promo_count_price_quartiles"),
        F.max("created_at").alias("last_created_at"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("product_id_tier").desc_nulls_last())
)
