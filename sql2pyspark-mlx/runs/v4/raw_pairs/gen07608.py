from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("product_id_whole").asc_nulls_first())
w2 = Window.partitionBy("promo").orderBy(F.col("price").desc_nulls_first())
w3 = Window.partitionBy("promo")
w4 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.partitionBy("promo").orderBy(F.col("promo_count_price").asc_nulls_first())

flattened = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

valid = (
    flattened
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & (F.col("created_at") < "2025-05-27"))
    .select(
        "*",
        F.floor("product_id").alias("product_id_whole"),
        F.coalesce("promo", F.lit("UNKNOWN")).alias("promo_clean"),
    )
)

metrics = (
    valid
    .withColumn("product_id_whole_cume_dist", F.cume_dist().over(w))
    .withColumn("price_cume_dist", F.cume_dist().over(w2))
    .withColumn("promo_count_price", F.count("price").over(w3))
)

metrics_carts = (
    metrics
    .select(
        "*",
        F.round((F.col("promo_count_price") / F.sum("promo_count_price").over(w3)), 4).alias("promo_count_price_share"),
        F.min("price_cume_dist").over(w3).alias("promo_min_price_cume_dist"),
    )
)

with_windows = (
    metrics_carts
    .select(
        "*",
        F.count("product_id_whole_cume_dist").over(w4).alias("product_id_whole_cume_dist_count_30m"),
        F.rank().over(w5).alias("promo_count_price_rank"),
        F.max("product_id").over(w3).alias("promo_max_product_id"),
    )
)

result = (
    with_windows
    .rollup(
        "product_id",
        F.when(F.col("promo_count_price") >= 15, "PREMIUM").otherwise("STANDARD").alias("promo_count_price_tier"),
    )
    .agg(
        F.sum(F.when(F.col("promo") == "VIP", F.col("promo_count_price_rank"))).alias("vip_total_promo_count_price_rank"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.max("created_at").alias("ended_at"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min_2"),
    )
)
