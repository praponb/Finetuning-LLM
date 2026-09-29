from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("product_id")
w2 = Window.partitionBy("segment").orderBy(F.col("qty").asc_nulls_last())
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("segment").orderBy(F.col("qty").asc_nulls_first())
w5 = Window.partitionBy("city").orderBy(F.col("qty").asc_nulls_last())
w6 = Window.partitionBy("promo")
w7 = Window.partitionBy("segment")

lines = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.promo"),
    )
)

enriched = (
    lines
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.segment"), F.col("b.email"), F.col("b.city"))
)

metrics = (
    enriched
    .select(
        "id",
        "customer_id",
        "qty",
        "promo",
        "segment",
        "email",
        "city",
        F.cume_dist().over(w).alias("product_id_cume_dist"),
        F.dense_rank().over(w2).alias("qty_dense_rank"),
        F.round((F.col("product_id") / F.sum("product_id").over(w3)), 4).alias("product_id_share"),
        F.rank().over(w4).alias("qty_rank"),
        F.rank().over(w5).alias("qty_rank_2"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "customer_id",
        "qty",
        "email",
        "city",
        "product_id_cume_dist",
        "qty_dense_rank",
        "product_id_share",
        "qty_rank_2",
        F.round((F.col("qty") / F.sum("qty").over(w6)), 4).alias("qty_share"),
        F.round((F.col("qty_dense_rank") / F.sum("qty_dense_rank").over(w7)), 4).alias("qty_dense_rank_share"),
        F.max("qty_rank_2").over(w6).alias("promo_max_qty_rank_2"),
    )
)

base = (
    with_windows
    .filter(~F.col("city").eqNullSafe(F.lit("Singapore")))
    .select(
        "id",
        "city",
        "product_id_cume_dist",
        "qty_dense_rank",
        "qty_rank_2",
        "qty_share",
        "qty_dense_rank_share",
        (F.col("qty_share") + F.col("qty_rank_2")).alias("qty_share_plus_qty_rank_2"),
    )
)

with_windows_carts = (
    base
    .withColumn("qty_dense_rank_share_2", F.round((F.col("qty_dense_rank") / F.sum("qty_dense_rank").over(w3)), 4))
)

result = (
    with_windows_carts
    .select(
        "id",
        "qty_rank_2",
        "qty_share",
        "qty_share_plus_qty_rank_2",
        "qty_dense_rank_share_2",
    )
    .orderBy(F.col("qty_rank_2").desc())
)
