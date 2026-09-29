from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("promo").orderBy(F.col("qty").desc_nulls_last())
w2 = Window.partitionBy("promo", "customer_id")
w_ordered_2 = Window.partitionBy("customer_id").orderBy(F.col("qty").desc())
w4 = Window.orderBy(F.col("qty_cume_dist").desc_nulls_first())
w_part = Window.orderBy("qty_cume_dist")
w_recent = Window.partitionBy("promo")
w_part_2 = Window.partitionBy("promo").orderBy(F.col("qty_cume_dist").desc())
w8 = Window.orderBy(F.col("qty_dense_rank").desc_nulls_first())
w9 = Window.partitionBy("promo").orderBy(F.col("promo_count_promo_count_qty").desc())
w10 = Window.partitionBy("promo").orderBy(F.col("qty_cume_dist_percent_rank").asc_nulls_last())
w11 = Window.orderBy(F.col("greatest_qty_dense_rank_qty_cume_dist").desc())

flattened = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

metrics = (
    flattened
    .select(
        "coupon_codes",
        "promo",
        F.dense_rank().over(w_ordered).alias("qty_dense_rank"),
        F.count("qty").over(w2).alias("promo_count_qty"),
        F.cume_dist().over(w_ordered_2).alias("qty_cume_dist"),
    )
)

metrics_carts = (
    metrics
    .filter(F.array_contains("coupon_codes", "BUNDLE"))
    .withColumn("qty_cume_dist_rank", F.rank().over(w4))
    .withColumn("qty_cume_dist_dense_rank", F.dense_rank().over(w_part))
    .withColumn("promo_count_promo_count_qty", F.count("promo_count_qty").over(w_recent))
    .withColumn("qty_cume_dist_percent_rank", F.percent_rank().over(w_part_2))
)

valid = (
    metrics_carts
    .filter(F.col("qty_dense_rank") > 4)
    .withColumn("greatest_qty_dense_rank_qty_cume_dist", F.greatest("qty_dense_rank", "qty_cume_dist"))
)

metrics_carts_2 = (
    valid
    .select(
        "*",
        F.rank().over(w8).alias("qty_dense_rank_rank"),
        F.percent_rank().over(w9).alias("promo_count_promo_count_qty_percent_rank"),
        F.rank().over(w10).alias("qty_cume_dist_percent_rank_rank"),
        F.dense_rank().over(w11).alias("greatest_qty_dense_rank_qty_cume_dist_dense_rank"),
    )
)

cleaned = (
    metrics_carts_2
    .filter(F.col("promo").isNotNull())
    .select(
        "coupon_codes",
        "qty_dense_rank",
        "qty_cume_dist_rank",
        "qty_cume_dist_dense_rank",
        "promo_count_promo_count_qty",
        "qty_cume_dist_percent_rank",
        "greatest_qty_dense_rank_qty_cume_dist",
        "qty_dense_rank_rank",
        "qty_cume_dist_percent_rank_rank",
        "greatest_qty_dense_rank_qty_cume_dist_dense_rank",
        F.col("qty_cume_dist_dense_rank").cast("double").alias("qty_cume_dist_dense_rank_str"),
        F.coalesce("promo", F.lit("UNKNOWN")).alias("promo_filled"),
    )
)

filtered = (
    cleaned
    .filter(F.col("greatest_qty_dense_rank_qty_cume_dist") > 29)
    .withColumn("greatest_qty_dense_rank_qty_cume_dist_dense_rank_adjusted", F.round((F.col("greatest_qty_dense_rank_qty_cume_dist_dense_rank") * 1.15), 2))
)

grouped = (
    filtered
    .groupBy(
        F.when(F.col("qty_cume_dist_dense_rank_str") >= 76, "HIGH").otherwise("LOW").alias("qty_cume_dist_dense_rank_str_tier"),
        F.when(F.col("qty_cume_dist_dense_rank") >= 8, "HIGH").otherwise("LOW").alias("qty_cume_dist_dense_rank_tier"),
        F.when(F.col("qty_dense_rank_rank") >= 8, "HIGH").otherwise("LOW").alias("qty_dense_rank_rank_tier"),
    )
    .agg(
        F.percentile_approx("qty_cume_dist_percent_rank", [0.25, 0.5, 0.75]).alias("qty_cume_dist_percent_rank_quartiles"),
        F.max("greatest_qty_dense_rank_qty_cume_dist_dense_rank").alias("max_greatest_qty_dense_rank_qty_cume_dist_dense_rank"),
        F.count("*").alias("cnt"),
    )
)

result = (
    grouped
    .select(
        "qty_cume_dist_dense_rank_str_tier",
        "qty_cume_dist_dense_rank_tier",
        "qty_dense_rank_rank_tier",
        "qty_cume_dist_percent_rank_quartiles",
        "max_greatest_qty_dense_rank_qty_cume_dist_dense_rank",
        "cnt",
    )
    .orderBy(
        F.col("max_greatest_qty_dense_rank_qty_cume_dist_dense_rank").desc_nulls_last(),
        "qty_cume_dist_dense_rank_tier",
    )
)
