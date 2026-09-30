from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.orderBy(F.col("product_id").desc_nulls_first())
w3 = Window.partitionBy("promo")
w4 = Window.partitionBy("promo", "customer_id").orderBy(F.col("product_id").asc_nulls_last())
w_running = Window.partitionBy("customer_id").orderBy(F.col("product_id_rank_2").desc_nulls_first())
w6 = Window.orderBy(F.col("product_id_rank_2_dense_rank").asc_nulls_last())
w7 = Window.orderBy(F.col("product_id_rank_2_dense_rank").desc_nulls_last())

lines = df_carts.select("id", "customer_id", F.inline("items"))

with_windows = (
    lines
    .select(
        "id",
        "customer_id",
        "qty",
        "promo",
        F.stddev_samp("price").over(w).alias("customer_id_stddev_samp_price"),
        F.rank().over(w2).alias("product_id_rank"),
        F.max("product_id").over(w3).alias("promo_max_product_id"),
        F.count("product_id").over(w3).alias("promo_count_product_id"),
        F.rank().over(w4).alias("product_id_rank_2"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "customer_id_stddev_samp_price",
        "promo_count_product_id",
        F.dense_rank().over(w_running).alias("product_id_rank_2_dense_rank"),
        F.round((F.col("product_id_rank_2") / F.sum("product_id_rank_2").over(w)), 4).alias("product_id_rank_2_share"),
        F.count("product_id_rank_2").over(w).alias("customer_id_count_product_id_rank_2"),
    )
)

with_windows_carts = (
    metrics
    .filter(F.col("customer_id_stddev_samp_price").isNotNull())
    .select(
        "*",
        F.percent_rank().over(w6).alias("product_id_rank_2_dense_rank_percent_rank"),
        F.cume_dist().over(w7).alias("product_id_rank_2_dense_rank_cume_dist"),
    )
)

result = (
    with_windows_carts
    .select(
        "id",
        "customer_id_stddev_samp_price",
        "promo_count_product_id",
        "product_id_rank_2_dense_rank",
        "product_id_rank_2_share",
        "customer_id_count_product_id_rank_2",
        "product_id_rank_2_dense_rank_percent_rank",
        "product_id_rank_2_dense_rank_cume_dist",
    )
    .orderBy(F.col("customer_id_count_product_id_rank_2").desc_nulls_last())
)
