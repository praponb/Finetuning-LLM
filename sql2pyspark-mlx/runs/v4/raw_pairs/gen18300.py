from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag")
w2 = Window.partitionBy("tag").orderBy(F.col("product_id").desc_nulls_last())
w3 = Window.orderBy("product_id_times_price")
w4 = Window.partitionBy("tag").orderBy(F.col("product_id_percent_rank").desc_nulls_first())
w5 = Window.partitionBy("tag").orderBy(F.col("pos").asc_nulls_first())

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "elem"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

lines = (
    flattened
    .select(
        "id",
        "pos",
        "product_id",
        "qty",
        "price",
        F.explode("coupon_codes").alias("tag"),
    )
)

base = (
    lines
    .filter(F.col("product_id") == 4)
    .select(
        "*",
        (F.col("product_id") * F.col("price")).alias("product_id_times_price"),
        F.when(F.col("product_id") >= 10, "High").when(F.col("product_id") >= 3, "Medium").otherwise("Low").alias("product_id_level"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.max("product_id").over(w).alias("tag_max_product_id"),
        F.round((F.col("qty") / F.sum("qty").over(w)), 4).alias("qty_share"),
        F.percent_rank().over(w2).alias("product_id_percent_rank"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.rank().over(w3).alias("product_id_times_price_rank"),
        F.percent_rank().over(w4).alias("product_id_percent_rank_percent_rank"),
        F.cume_dist().over(w5).alias("pos_cume_dist"),
    )
)

result = (
    scored
    .select(
        "product_id",
        "price",
        "tag",
        "product_id_times_price",
        "product_id_level",
        "tag_max_product_id",
        "qty_share",
        "product_id_percent_rank",
        "product_id_percent_rank_percent_rank",
    )
    .orderBy(F.col("product_id_times_price").asc_nulls_last())
)
