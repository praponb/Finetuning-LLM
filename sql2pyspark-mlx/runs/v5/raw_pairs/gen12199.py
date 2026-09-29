from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "rec"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        (F.col("pos") + 1).alias("position"),
        F.col("rec.product_id"),
        F.col("rec.qty"),
        F.col("rec.price"),
        F.col("rec.promo"),
    )
)

filtered = (
    flattened
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-01"))) & F.col("promo").isNotNull())
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "product_id",
        "qty",
        "price",
        "promo",
        (F.col("qty") - F.col("product_id")).alias("qty_minus_product_id"),
        F.round((F.col("price") * 1.15), 2).alias("price_scaled"),
    )
)

enriched = (
    filtered
    .withColumn("promo_sum_product_id", F.sum("product_id").over(w))
    .withColumn("qty_max_2h", F.max("qty").over(w2))
    .withColumn("promo_max_qty_minus_product_id", F.max("qty_minus_product_id").over(w))
)

with_windows = (
    enriched
    .filter(F.col("promo_max_qty_minus_product_id") <= 62)
    .select(
        "created_at",
        "coupon_codes",
        "product_id",
        "qty",
        "price",
        "qty_minus_product_id",
        "promo_sum_product_id",
        "qty_max_2h",
        "promo_max_qty_minus_product_id",
        F.count("price_scaled").over(w3).alias("price_scaled_count_15m"),
        F.stddev_samp("qty").over(w).alias("promo_stddev_samp_qty"),
        F.avg("qty").over(w4).alias("qty_avg_2h"),
        F.max("price").over(w5).alias("price_max_30m"),
    )
)

cleaned = (
    with_windows
    .filter((F.col("created_at") > F.lit("2025-03-01 20:00:00").cast("timestamp")) & (F.col("product_id") <= 7))
    .select(
        "coupon_codes",
        "qty",
        "price",
        "promo_sum_product_id",
        "qty_max_2h",
        "promo_max_qty_minus_product_id",
        "price_scaled_count_15m",
        "promo_stddev_samp_qty",
        "qty_avg_2h",
        "price_max_30m",
        (F.col("qty") * F.col("promo_max_qty_minus_product_id")).alias("qty_times_promo_max_qty_minus_product_id"),
    )
)

result = (
    cleaned
    .select(
        "qty",
        "price",
        "promo_max_qty_minus_product_id",
        "price_scaled_count_15m",
        "promo_stddev_samp_qty",
        "qty_avg_2h",
    )
    .orderBy("price")
)
