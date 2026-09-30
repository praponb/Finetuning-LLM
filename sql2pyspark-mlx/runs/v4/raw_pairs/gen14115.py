from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").desc())
w2 = Window.orderBy("qty")
w3 = Window.orderBy("price")
w4 = Window.orderBy("qty_rank")
w5 = Window.orderBy(F.col("qty_rank").desc_nulls_first())
w6 = Window.orderBy(F.col("qty_rank_dense_rank").asc_nulls_last())
w7 = Window.orderBy(F.col("price_dense_rank").desc())
w8 = Window.orderBy(F.col("qty_rank").desc())
w9 = Window.orderBy(F.col("price_cume_dist_2").desc_nulls_last())
w10 = Window.orderBy(F.col("price_cume_dist").asc_nulls_last())
w_running = Window.orderBy("price_cume_dist_2")
w12 = Window.orderBy(F.col("price_cume_dist").desc())
w13 = Window.orderBy(F.col("price").desc_nulls_last())
w14 = Window.orderBy(F.col("price_cume_dist_2_rank").desc_nulls_last())
w15 = Window.orderBy(F.col("price_cume_dist_2_rank").asc_nulls_last())
w16 = Window.orderBy("price_cume_dist_dense_rank")
w17 = Window.orderBy("price_cume_dist_2_percent_rank")

exploded = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
    )
)

enriched = (
    exploded
    .withColumn("price_cume_dist", F.cume_dist().over(w))
    .withColumn("qty_rank", F.rank().over(w2))
    .withColumn("price_dense_rank", F.dense_rank().over(w3))
    .withColumn("price_cume_dist_2", F.cume_dist().over(w))
)

kept_keys = (
    enriched
    .select("id")
    .subtract(
        df_carts
        .filter(F.col("customer_id") == 35)
        .select("id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_carts = (
    reconciled
    .withColumn("price_dense_rank_2", F.dense_rank().over(w))
    .withColumn("qty_rank_rank", F.rank().over(w4))
    .withColumn("qty_rank_dense_rank", F.dense_rank().over(w5))
)

enriched_carts_2 = (
    enriched_carts
    .withColumn("qty_rank_dense_rank_percent_rank", F.percent_rank().over(w6))
    .withColumn("price_dense_rank_percent_rank", F.percent_rank().over(w7))
    .withColumn("qty_rank_dense_rank_2", F.dense_rank().over(w8))
)

scored = (
    enriched_carts_2
    .withColumn("price_cume_dist_2_percent_rank", F.percent_rank().over(w9))
    .withColumn("price_cume_dist_dense_rank", F.dense_rank().over(w10))
    .withColumn("price_cume_dist_2_rank", F.rank().over(w_running))
    .withColumn("price_cume_dist_percent_rank", F.percent_rank().over(w12))
)

with_windows = (
    scored
    .select(
        "*",
        F.dense_rank().over(w13).alias("price_dense_rank_3"),
        F.percent_rank().over(w14).alias("price_cume_dist_2_rank_percent_rank"),
    )
)

result = (
    with_windows
    .withColumn("price_cume_dist_2_rank_percent_rank_2", F.percent_rank().over(w15))
    .withColumn("price_cume_dist_dense_rank_percent_rank", F.percent_rank().over(w16))
    .withColumn("price_cume_dist_2_percent_rank_rank", F.rank().over(w17))
)
