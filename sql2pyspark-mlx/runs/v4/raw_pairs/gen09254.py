from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("promo").orderBy(F.col("product_id").desc_nulls_last())
w4 = Window.orderBy(F.col("product_id").desc_nulls_first())

lines = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.promo"),
    )
)

scored = (
    lines
    .select(
        "*",
        F.round((F.col("qty") / F.sum("qty").over(w)), 4).alias("qty_share"),
        F.avg("product_id").over(w2).alias("product_id_avg_1h"),
        F.rank().over(w3).alias("product_id_rank"),
    )
)

cleaned = (
    scored
    .filter((F.col("created_at") >= F.lit("2025-03-02 07:00:00").cast("timestamp")) & F.col("promo").isNotNull() & (F.col("product_id_avg_1h") <= 9))
    .select(
        "*",
        (F.col("product_id_rank") * F.col("qty")).alias("product_id_rank_times_qty"),
        F.coalesce("promo", F.lit("DIRECT")).alias("promo_clean"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.rank().over(w4).alias("product_id_rank_2"),
        F.stddev_samp("qty_share").over(w).alias("promo_stddev_samp_qty_share"),
        F.round((F.col("product_id_avg_1h") / F.sum("product_id_avg_1h").over(w)), 4).alias("product_id_avg_1h_share"),
    )
)

with_windows_carts = (
    with_windows
    .select(
        "*",
        F.max("product_id_rank_times_qty").over(w2).alias("product_id_rank_times_qty_max_1h"),
        F.stddev_samp("product_id_avg_1h_share").over(w).alias("promo_stddev_samp_product_id_avg_1h_share"),
    )
)

valid = (
    with_windows_carts
    .filter(F.col("promo").isNotNull() & (F.col("qty_share") >= 0.22))
    .select(
        "*",
        F.greatest("promo_stddev_samp_qty_share", "product_id_avg_1h_share").alias("greatest_promo_stddev_samp_qty_share_product_id_avg_1h_share"),
        F.coalesce("promo", F.lit("UNKNOWN")).alias("resolved_promo"),
    )
)

cleaned_carts = (
    valid
    .filter(F.col("product_id_avg_1h") >= 5)
    .select(
        "product_id",
        "qty",
        "promo",
        "qty_share",
        "product_id_avg_1h",
        "product_id_rank",
        "product_id_rank_times_qty",
        "product_id_rank_2",
        "promo_stddev_samp_qty_share",
        "product_id_avg_1h_share",
        "promo_stddev_samp_product_id_avg_1h_share",
        "greatest_promo_stddev_samp_qty_share_product_id_avg_1h_share",
        "resolved_promo",
        F.least("product_id_rank", "product_id").alias("least_product_id_rank_product_id"),
    )
)

result = (
    cleaned_carts
    .select(
        "product_id",
        "qty",
        "promo",
        "qty_share",
        "product_id_rank_times_qty",
        "product_id_rank_2",
        "product_id_avg_1h_share",
        "promo_stddev_samp_product_id_avg_1h_share",
        F.when(F.col("promo").rlike("^(WELCOME5|BUNDLE)$"), "EXTENDED").when(F.col("promo").rlike("^(SPRING10|VIP)$"), "GROUP_B").otherwise("CORE").alias("promo_zone"),
    )
    .orderBy(F.col("promo_stddev_samp_product_id_avg_1h_share").desc_nulls_first())
)
