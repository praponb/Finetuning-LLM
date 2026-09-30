from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("customer_id")
w3 = Window.orderBy(F.col("product_id").asc_nulls_first())
w4 = Window.partitionBy("promo").orderBy(F.col("price").desc_nulls_first())
w5 = Window.partitionBy("promo").orderBy(F.col("price_cume_dist").desc_nulls_first())

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "li_item"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "pos",
        F.col("li_item.product_id"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

matched_keys = (
    exploded
    .select("id")
    .intersect(
        df_carts
        .filter(F.col("customer_id").isNotNull())
        .select("id")
    )
)

kept_rows = (
    exploded
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .withColumn("promo_avg_price", F.avg("price").over(w))
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w2)), 4))
)

enriched = (
    metrics
    .withColumn("product_id_dense_rank", F.dense_rank().over(w3))
    .withColumn("promo_count_price_share", F.count("price_share").over(w))
    .withColumn("price_cume_dist", F.cume_dist().over(w4))
)

cleaned = (
    enriched
    .filter(F.col("promo").isNotNull() & F.col("customer_id").isin([1, 28]))
    .select(
        "id",
        "coupon_codes",
        "pos",
        "product_id",
        "promo",
        "promo_avg_price",
        "price_share",
        "product_id_dense_rank",
        "promo_count_price_share",
        "price_cume_dist",
        F.round("pos", 2).alias("pos_rounded"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.percent_rank().over(w5).alias("price_cume_dist_percent_rank"),
        F.avg("price_share").over(w).alias("promo_avg_price_share"),
        F.round((F.col("product_id_dense_rank") / F.sum("product_id_dense_rank").over(w)), 4).alias("product_id_dense_rank_share"),
        F.avg("product_id_dense_rank").over(w).alias("promo_avg_product_id_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "coupon_codes",
        "pos",
        "product_id",
        "promo",
        "price_share",
        "promo_count_price_share",
        "pos_rounded",
        "promo_avg_price_share",
    )
    .orderBy(F.col("promo_count_price_share").desc())
)
