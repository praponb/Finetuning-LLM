from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").desc_nulls_last())
w2 = Window.orderBy("product_id")
w3 = Window.orderBy(F.col("product_id").desc_nulls_last())
w4 = Window.orderBy(F.col("price").desc())
w5 = Window.orderBy(F.col("price_rank").desc_nulls_last())
w_ordered = Window.orderBy(F.col("product_id").asc_nulls_last())
w7 = Window.orderBy(F.col("product_id_cume_dist").desc_nulls_first())
w8 = Window.orderBy(F.col("price_rank_cume_dist").desc_nulls_first())
w9 = Window.orderBy(F.col("price_rank_cume_dist").asc_nulls_last())
w10 = Window.partitionBy("product_id_rank_2_tier").orderBy(F.col("price_rank_cume_dist_rank_p50").desc())

flattened = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
    )
)

scored = (
    flattened
    .select(
        "*",
        F.rank().over(w).alias("price_rank"),
        F.rank().over(w2).alias("product_id_rank"),
        F.cume_dist().over(w3).alias("product_id_cume_dist"),
        F.rank().over(w4).alias("price_rank_2"),
        F.cume_dist().over(w).alias("price_cume_dist"),
    )
)

matched_keys = (
    scored
    .select("id")
    .subtract(
        df_carts
        .filter(F.col("customer_id").isNull())
        .select("id")
    )
)

verified = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "id",
        "product_id_cume_dist",
        F.dense_rank().over(w4).alias("price_dense_rank"),
        F.cume_dist().over(w5).alias("price_rank_cume_dist"),
        F.rank().over(w_ordered).alias("product_id_rank_2"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "product_id_cume_dist",
        "price_dense_rank",
        "price_rank_cume_dist",
        "product_id_rank_2",
        F.dense_rank().over(w7).alias("product_id_cume_dist_dense_rank"),
        F.rank().over(w8).alias("price_rank_cume_dist_rank"),
        F.rank().over(w9).alias("price_rank_cume_dist_rank_2"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("product_id_rank_2") >= 7, "PREMIUM").otherwise("STANDARD").alias("product_id_rank_2_tier"),
        F.when(F.col("price_rank_cume_dist_rank_2") >= 8, "LARGE").otherwise("SMALL").alias("price_rank_cume_dist_rank_2_tier"),
    )
    .agg(
        F.percentile_approx("product_id_cume_dist_dense_rank", 0.9).alias("product_id_cume_dist_dense_rank_p90"),
        F.percentile_approx("price_dense_rank", [0.25, 0.5, 0.75]).alias("price_dense_rank_quartiles"),
        F.percentile_approx("price_rank_cume_dist_rank", 0.5).alias("price_rank_cume_dist_rank_p50"),
        F.percentile_approx("price_rank_cume_dist_rank", [0.25, 0.5, 0.75]).alias("price_rank_cume_dist_rank_quartiles"),
        F.percentile_approx("price_dense_rank", [0.25, 0.5, 0.75]).alias("price_dense_rank_quartiles_2"),
    )
    .withColumn("price_rank_cume_dist_rank_p50_rank", F.rank().over(w10))
    .orderBy(F.col("price_rank_cume_dist_rank_p50").desc_nulls_last())
)
