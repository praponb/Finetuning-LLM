from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("promo")
w_recent = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.orderBy(F.col("price_avg_30m").asc_nulls_first())
w4 = Window.orderBy(F.col("qty").desc_nulls_last())
w5 = Window.orderBy(F.col("price_avg_30m").desc())
w6 = Window.orderBy(F.col("qty_cume_dist").desc())
w7 = Window.orderBy(F.col("price_avg_30m_cume_dist").desc())
w_hist = Window.orderBy(F.col("price_avg_30m_dense_rank").desc_nulls_first())
w9 = Window.orderBy(F.col("qty_cume_dist").desc_nulls_first())
w_ordered = Window.orderBy(F.col("price_avg_30m_dense_rank_dense_rank").desc_nulls_first())
w11 = Window.orderBy(F.col("price_avg_30m_dense_rank_2").asc_nulls_first())
w12 = Window.orderBy(F.col("price_avg_30m_dense_rank_dense_rank").asc_nulls_first())
w13 = Window.orderBy(F.col("price_avg_30m_cume_dist_cume_dist").asc_nulls_first())

exploded = (
    df_carts
    .withColumn("entry", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("entry.product_id"),
        F.col("entry.qty"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)

with_windows = (
    exploded
    .select(
        "id",
        "qty",
        "price",
        F.avg("price").over(w_grp).alias("promo_avg_price"),
        F.avg("price").over(w_recent).alias("price_avg_30m"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "qty",
        "price",
        "promo_avg_price",
        "price_avg_30m",
        F.cume_dist().over(w3).alias("price_avg_30m_cume_dist"),
        F.cume_dist().over(w4).alias("qty_cume_dist"),
        F.dense_rank().over(w5).alias("price_avg_30m_dense_rank"),
    )
)

scored_carts = (
    scored
    .filter(F.col("price_avg_30m_cume_dist") > 0.69)
    .select(
        "qty",
        "price",
        "price_avg_30m_cume_dist",
        "qty_cume_dist",
        F.percent_rank().over(w6).alias("qty_cume_dist_percent_rank"),
        F.cume_dist().over(w7).alias("price_avg_30m_cume_dist_cume_dist"),
        F.dense_rank().over(w_hist).alias("price_avg_30m_dense_rank_dense_rank"),
        F.dense_rank().over(w5).alias("price_avg_30m_dense_rank_2"),
    )
)

enriched = (
    scored_carts
    .select(
        "qty",
        "price_avg_30m_dense_rank_2",
        F.percent_rank().over(w9).alias("qty_cume_dist_percent_rank_2"),
        F.rank().over(w_ordered).alias("price_avg_30m_dense_rank_dense_rank_rank"),
        F.dense_rank().over(w11).alias("price_avg_30m_dense_rank_2_dense_rank"),
        F.dense_rank().over(w12).alias("price_avg_30m_dense_rank_dense_rank_dense_rank"),
        F.rank().over(w13).alias("price_avg_30m_cume_dist_cume_dist_rank"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("price_avg_30m_dense_rank_dense_rank_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("price_avg_30m_dense_rank_dense_rank_rank_tier"),
    )
    .agg(
        F.percentile_approx("price_avg_30m_dense_rank_2", [0.25, 0.5, 0.75]).alias("price_avg_30m_dense_rank_2_quartiles"),
        F.percentile_approx("price_avg_30m_dense_rank_dense_rank_dense_rank", [0.25, 0.5, 0.75]).alias("price_avg_30m_dense_rank_dense_rank_dense_rank_quartiles"),
        F.percentile_approx("qty_cume_dist_percent_rank_2", [0.25, 0.5, 0.75]).alias("qty_cume_dist_percent_rank_2_quartiles"),
    )
    .orderBy(F.col("price_avg_30m_dense_rank_dense_rank_rank_tier").desc_nulls_first())
)
