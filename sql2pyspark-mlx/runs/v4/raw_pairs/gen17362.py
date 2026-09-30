from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w_hist = Window.partitionBy("promo").orderBy(F.col("price").desc(), "id", "pos")
w3 = Window.partitionBy("promo").orderBy("pos", "id", "pos")

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "elem"))
    .select(
        "id",
        "coupon_codes",
        "pos",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

enriched = (
    exploded
    .select(
        "*",
        F.round((F.col("product_id") / F.sum("product_id").over(w)), 4).alias("product_id_share"),
        F.ntile(3).over(w_hist).alias("price_bucket"),
        F.count("pos").over(w).alias("promo_count_pos"),
        F.stddev_samp("price").over(w).alias("promo_stddev_samp_price"),
        F.lag("product_id", 1, 0).over(w3).alias("prev_product_id"),
    )
)

cleaned = (
    enriched
    .filter(F.col("product_id").isNull())
    .select(
        "id",
        "coupon_codes",
        "pos",
        "product_id",
        "promo",
        "promo_count_pos",
        "promo_stddev_samp_price",
        F.least("prev_product_id", "pos").alias("least_prev_product_id_pos"),
    )
)

filtered = (
    cleaned
    .filter(F.col("promo").isNotNull() & (F.col("promo_stddev_samp_price") <= 106))
    .select(
        "id",
        "coupon_codes",
        "pos",
        "product_id",
        "promo",
        "least_prev_product_id_pos",
        F.round((F.col("promo_count_pos") / F.col("promo_stddev_samp_price")), 2).alias("promo_count_pos_per_promo_stddev_samp_price"),
        F.array_join("coupon_codes", ", ").alias("coupon_codes_text"),
        F.coalesce("promo", F.lit("N/A")).alias("promo_filled"),
    )
)

result = (
    filtered
    .groupBy("promo")
    .agg(
        F.count(F.when(F.col("promo") == "SPRING10", 1)).alias("spring10_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("promo") == "BUNDLE", F.col("promo_count_pos_per_promo_stddev_samp_price")).otherwise(0)).alias("bundle_promo_count_pos_per_promo_stddev_samp_price"),
        F.max(F.when(F.col("promo") == "VIP", F.col("promo_count_pos_per_promo_stddev_samp_price"))).alias("vip_max_promo_count_pos_per_promo_stddev_samp_price"),
        F.round(F.avg("promo_count_pos_per_promo_stddev_samp_price"), 2).alias("avg_promo_count_pos_per_promo_stddev_samp_price"),
        F.percentile_approx("pos", 0.95).alias("pos_p95"),
    )
    .filter(F.col("spring10_rows") < 1)
)
