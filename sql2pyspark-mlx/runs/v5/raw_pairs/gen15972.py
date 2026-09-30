from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.orderBy(F.col("price").asc_nulls_first())
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.orderBy(F.col("product_id").asc_nulls_last())
w5 = Window.partitionBy("promo")
w_part = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w7 = Window.partitionBy("promo").orderBy(F.col("price_rank").asc_nulls_last())

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "li_item"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

with_windows = (
    flattened
    .withColumn("price_avg_15m", F.avg("price").over(w))
    .withColumn("price_rank", F.rank().over(w2))
    .withColumn("product_id_avg_1h", F.avg("product_id").over(w3))
    .withColumn("product_id_cume_dist", F.cume_dist().over(w4))
)

with_windows_carts = (
    with_windows
    .filter(F.col("product_id") <= 9)
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "product_id",
        "qty",
        "promo",
        "price_rank",
        "product_id_avg_1h",
        "product_id_cume_dist",
        F.round((F.col("price_avg_15m") / F.sum("price_avg_15m").over(w5)), 4).alias("price_avg_15m_share"),
        F.round((F.col("price") / F.sum("price").over(w5)), 4).alias("price_share"),
        F.avg("pos").over(w_part).alias("pos_avg_30m"),
        F.round((F.col("qty") / F.sum("qty").over(w5)), 4).alias("qty_share"),
        F.count("pos").over(w5).alias("promo_count_pos"),
    )
)

metrics = (
    with_windows_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "product_id",
        "promo",
        "product_id_avg_1h",
        "qty_share",
        F.percent_rank().over(w7).alias("price_rank_percent_rank"),
    )
)

cleaned = (
    metrics
    .filter(F.col("promo").isNotNull())
    .withColumn("clean_promo", F.trim("promo"))
    .withColumn("resolved_promo", F.coalesce("promo", F.lit("DIRECT")))
)

result = (
    cleaned
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.count(F.when(F.col("promo") == "SPRING10", 1)).alias("spring10_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("promo") == "BUNDLE", F.col("product_id_avg_1h")).otherwise(0)).alias("bundle_product_id_avg_1h"),
    )
    .orderBy(F.col("bundle_product_id_avg_1h").desc())
)
