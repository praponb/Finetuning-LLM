from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("product_id").asc_nulls_last())
w2 = Window.partitionBy("promo").orderBy("qty")
w3 = Window.partitionBy("promo")
w4 = Window.orderBy("product_id_cume_dist")

exploded = (
    df_carts
    .withColumn("li_item", F.explode_outer("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.promo"),
    )
)

enriched = (
    exploded
    .select(
        "id",
        "coupon_codes",
        "product_id",
        "qty",
        "promo",
        F.cume_dist().over(w).alias("product_id_cume_dist"),
        F.cume_dist().over(w2).alias("qty_cume_dist"),
        F.round((F.col("product_id") / F.sum("product_id").over(w3)), 4).alias("product_id_share"),
        F.round((F.col("product_id") / F.sum("product_id").over(w3)), 4).alias("product_id_share_2"),
        F.round((F.col("product_id") / F.sum("product_id").over(w3)), 4).alias("product_id_share_3"),
    )
)

cleaned = (
    enriched
    .filter(F.col("promo").isNotNull())
    .select(
        "*",
        F.ceil("qty").alias("qty_whole"),
        F.coalesce("promo", F.lit("Unknown")).alias("promo_filled"),
    )
)

enriched_carts = (
    cleaned
    .withColumn("promo_stddev_samp_product_id_share_3", F.stddev_samp("product_id_share_3").over(w3))
    .withColumn("product_id_cume_dist_percent_rank", F.percent_rank().over(w4))
)

filtered = (
    enriched_carts
    .filter(F.col("promo").eqNullSafe(F.lit("WELCOME5")))
    .select(
        "coupon_codes",
        "product_id",
        "promo",
        "product_id_cume_dist",
        "qty_cume_dist",
        "product_id_share_2",
        "product_id_share_3",
        "promo_stddev_samp_product_id_share_3",
        (F.col("product_id_cume_dist") - F.col("qty")).alias("product_id_cume_dist_minus_qty"),
        F.coalesce("promo", F.lit("N/A")).alias("promo_clean"),
    )
)

report = (
    filtered
    .select(
        "product_id",
        "promo",
        "product_id_share_2",
        "product_id_share_3",
        "promo_stddev_samp_product_id_share_3",
        "product_id_cume_dist_minus_qty",
        "promo_clean",
    )
    .orderBy(F.col("promo_stddev_samp_product_id_share_3").desc_nulls_first())
)

result = (
    report
    .select(
        "promo",
        "product_id_share_2",
        "product_id_share_3",
        "promo_stddev_samp_product_id_share_3",
        F.when(F.col("promo").rlike("^(WELCOME5|SPRING10)$"), "CORE").when(F.col("promo").rlike("^(VIP|BUNDLE)$"), "GROUP_B").otherwise("GROUP_A").alias("promo_zone"),
    )
    .orderBy(
        F.col("promo").desc(),
        F.col("promo_stddev_samp_product_id_share_3").desc(),
    )
)
