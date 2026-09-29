from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.orderBy(F.col("price").desc_nulls_first())
w3 = Window.partitionBy("promo")
w4 = Window.partitionBy("promo").orderBy(F.col("price_avg_1h").desc())
w5 = Window.orderBy(F.col("qty").asc_nulls_first())
w6 = Window.orderBy(F.col("promo_sum_price_dense_rank").asc_nulls_last())
w7 = Window.partitionBy("promo").orderBy(F.col("qty_cume_dist").desc_nulls_first())
w8 = Window.orderBy(F.col("qty_cume_dist_cume_dist").desc())
w9 = Window.orderBy(F.col("qty").desc_nulls_last())
w10 = Window.orderBy("price_dense_rank")
w11 = Window.orderBy(F.col("price_dense_rank").desc_nulls_last())
w12 = Window.orderBy(F.col("promo_sum_price_dense_rank_rank").asc_nulls_first())

exploded = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

with_windows = (
    exploded
    .select(
        "qty",
        "promo",
        F.avg("price").over(w).alias("price_avg_1h"),
        F.dense_rank().over(w2).alias("price_dense_rank"),
        F.sum("product_id").over(w3).alias("promo_sum_product_id"),
    )
)

with_windows_carts = (
    with_windows
    .filter(F.col("price_avg_1h") > 65)
    .select(
        "*",
        F.percent_rank().over(w4).alias("price_avg_1h_percent_rank"),
        F.round((F.col("price_avg_1h") / F.sum("price_avg_1h").over(w3)), 4).alias("price_avg_1h_share"),
        F.sum("price_avg_1h").over(w3).alias("promo_sum_price_avg_1h"),
        F.cume_dist().over(w5).alias("qty_cume_dist"),
        F.sum("price_dense_rank").over(w3).alias("promo_sum_price_dense_rank"),
    )
)

scored = (
    with_windows_carts
    .select(
        "qty",
        "price_dense_rank",
        "price_avg_1h_percent_rank",
        "promo_sum_price_dense_rank",
        F.rank().over(w6).alias("promo_sum_price_dense_rank_rank"),
        F.cume_dist().over(w7).alias("qty_cume_dist_cume_dist"),
    )
)

metrics = (
    scored
    .select(
        "price_dense_rank",
        "promo_sum_price_dense_rank",
        F.percent_rank().over(w8).alias("qty_cume_dist_cume_dist_percent_rank"),
        F.dense_rank().over(w9).alias("qty_dense_rank"),
        F.percent_rank().over(w10).alias("price_dense_rank_percent_rank"),
        F.cume_dist().over(w11).alias("price_dense_rank_cume_dist"),
        F.dense_rank().over(w12).alias("promo_sum_price_dense_rank_rank_dense_rank"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("price_dense_rank_percent_rank") >= 0.78, "LARGE").otherwise("SMALL").alias("price_dense_rank_percent_rank_tier"),
        F.when(F.col("price_dense_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("price_dense_rank_tier"),
        F.when(F.col("qty_cume_dist_cume_dist_percent_rank") >= 0.73, "PREMIUM").otherwise("STANDARD").alias("qty_cume_dist_cume_dist_percent_rank_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("qty_cume_dist_cume_dist_percent_rank", 0.5).alias("qty_cume_dist_cume_dist_percent_rank_p50"),
        F.percentile_approx("price_dense_rank_cume_dist", [0.25, 0.5, 0.75]).alias("price_dense_rank_cume_dist_quartiles"),
        F.max("qty_cume_dist_cume_dist_percent_rank").alias("max_qty_cume_dist_cume_dist_percent_rank"),
    )
    .orderBy(F.col("cnt").asc_nulls_last())
)
