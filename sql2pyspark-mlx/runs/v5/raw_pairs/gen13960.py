from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("qty")
w_hist = Window.orderBy(F.col("price").desc())
w3 = Window.orderBy(F.col("qty_percent_rank").desc())
w4 = Window.orderBy(F.col("qty_rank").desc_nulls_first())
w5 = Window.orderBy(F.col("qty_percent_rank_cume_dist").desc_nulls_first())
w6 = Window.orderBy(F.col("qty").desc_nulls_last())
w7 = Window.orderBy(F.col("qty_percent_rank_2").desc_nulls_first())
w8 = Window.orderBy("qty_percent_rank_cume_dist_dense_rank_plus_qty_percent_rank_cume_dist")
w9 = Window.orderBy(F.col("qty_percent_rank_cume_dist").asc_nulls_last())
w10 = Window.orderBy(F.col("qty_rank_rank").asc_nulls_last())
w11 = Window.orderBy(F.col("qty_percent_rank_cume_dist").desc_nulls_last())

flattened = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select("id", "coupon_codes", F.col("elem.qty"), F.col("elem.price"))
)

with_windows = (
    flattened
    .select(
        "id",
        "qty",
        F.percent_rank().over(w).alias("qty_percent_rank"),
        F.percent_rank().over(w_hist).alias("price_percent_rank"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "qty",
        "qty_percent_rank",
        "price_percent_rank",
        F.rank().over(w).alias("qty_rank"),
        F.cume_dist().over(w3).alias("qty_percent_rank_cume_dist"),
    )
)

metrics = (
    enriched
    .select(
        "price_percent_rank",
        "qty_rank",
        "qty_percent_rank_cume_dist",
        F.rank().over(w4).alias("qty_rank_rank"),
        F.dense_rank().over(w5).alias("qty_percent_rank_cume_dist_dense_rank"),
        F.percent_rank().over(w6).alias("qty_percent_rank_2"),
    )
)

valid = (
    metrics
    .filter(F.col("qty_percent_rank_cume_dist_dense_rank").between(3, 6))
    .withColumn("qty_percent_rank_cume_dist_dense_rank_plus_qty_percent_rank_cume_dist", (F.col("qty_percent_rank_cume_dist_dense_rank") + F.col("qty_percent_rank_cume_dist")))
    .withColumn("price_percent_rank_filled", F.coalesce("price_percent_rank", F.lit(0)))
)

metrics_carts = (
    valid
    .filter(F.col("price_percent_rank_filled") >= 49)
    .withColumn("qty_percent_rank_2_dense_rank", F.dense_rank().over(w7))
    .withColumn("qty_percent_rank_cume_dist_dense_rank_plus_qty_percent_rank_cume_dist_percent_rank", F.percent_rank().over(w8))
    .withColumn("qty_percent_rank_cume_dist_rank", F.rank().over(w9))
    .withColumn("qty_rank_rank_cume_dist", F.cume_dist().over(w10))
    .withColumn("qty_percent_rank_cume_dist_rank_2", F.rank().over(w11))
)

filtered = (
    metrics_carts
    .filter(F.col("qty_percent_rank_cume_dist_dense_rank").isNotNull())
    .select(
        "*",
        (F.col("qty_rank_rank") / F.nullif("qty_percent_rank_cume_dist", F.lit(0))).alias("qty_rank_rank_per_qty_percent_rank_cume_dist"),
        F.round("qty_percent_rank_cume_dist", 2).alias("qty_percent_rank_cume_dist_rounded"),
    )
)

stats = (
    filtered
    .groupBy(
        F.when(F.col("qty_percent_rank_cume_dist_dense_rank_plus_qty_percent_rank_cume_dist_percent_rank") >= 0.73, "HIGH").otherwise("LOW").alias("qty_percent_rank_cume_dist_dense_rank_plus_qty_percent_rank_cume_dist_percent_rank_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("qty_percent_rank_cume_dist_dense_rank", 0.9).alias("qty_percent_rank_cume_dist_dense_rank_p90"),
        F.max("qty_percent_rank_cume_dist_rounded").alias("max_qty_percent_rank_cume_dist_rounded"),
        F.avg("qty_rank_rank_cume_dist").alias("avg_qty_rank_rank_cume_dist"),
    )
)

result = (
    stats
    .select(
        "qty_percent_rank_cume_dist_dense_rank_plus_qty_percent_rank_cume_dist_percent_rank_tier",
    )
    .orderBy(
        F.col("qty_percent_rank_cume_dist_dense_rank_plus_qty_percent_rank_cume_dist_percent_rank_tier").desc_nulls_first(),
    )
    .limit(20)
)
