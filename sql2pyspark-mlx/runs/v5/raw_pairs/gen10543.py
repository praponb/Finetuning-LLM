from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").asc_nulls_last())
w2 = Window.orderBy(F.col("product_id").desc())
w3 = Window.orderBy(F.col("qty").desc_nulls_first())
w4 = Window.orderBy("qty")
w5 = Window.orderBy(F.col("product_id").asc_nulls_first())
w6 = Window.orderBy(F.col("price_cume_dist").asc_nulls_first())
w7 = Window.orderBy(F.col("price_cume_dist").desc_nulls_first())
w8 = Window.orderBy(F.col("price_cume_dist").desc_nulls_last())
w9 = Window.orderBy(F.col("price_cume_dist_cume_dist_2").asc_nulls_last())

exploded = (
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

metrics = (
    exploded
    .select(
        "product_id",
        "qty",
        F.cume_dist().over(w).alias("price_cume_dist"),
        F.cume_dist().over(w2).alias("product_id_cume_dist"),
    )
)

with_windows = (
    metrics
    .select(
        "product_id",
        "price_cume_dist",
        "product_id_cume_dist",
        F.rank().over(w3).alias("qty_rank"),
        F.percent_rank().over(w4).alias("qty_percent_rank"),
        F.dense_rank().over(w5).alias("product_id_dense_rank"),
    )
)

with_windows_carts = (
    with_windows
    .filter(F.col("product_id_cume_dist") < 0.51)
    .select(
        "price_cume_dist",
        "qty_rank",
        "product_id_dense_rank",
        F.cume_dist().over(w6).alias("price_cume_dist_cume_dist"),
        F.cume_dist().over(w7).alias("price_cume_dist_cume_dist_2"),
        F.dense_rank().over(w8).alias("price_cume_dist_dense_rank"),
    )
)

enriched = (
    with_windows_carts
    .select("*", F.cume_dist().over(w9).alias("price_cume_dist_cume_dist_2_cume_dist"))
)

base = (
    enriched
    .filter(F.col("qty_rank") == 6)
    .withColumn("price_cume_dist_adjusted", F.round((F.col("price_cume_dist") * 1.1), 2))
)

result = (
    base
    .withColumn("price_cume_dist_cume_dist_tier", F.when(F.col("price_cume_dist_cume_dist") >= 0.76, "LARGE").otherwise("SMALL"))
    .withColumn("qty_rank_tier", F.when(F.col("qty_rank") >= 8, "LARGE").otherwise("SMALL"))
    .rollup("price_cume_dist_cume_dist_tier", "qty_rank_tier")
    .agg(
        F.avg("price_cume_dist").alias("avg_price_cume_dist"),
        F.percentile_approx("price_cume_dist_cume_dist_2", [0.25, 0.5, 0.75]).alias("price_cume_dist_cume_dist_2_quartiles"),
        F.sum("price_cume_dist_cume_dist_2_cume_dist").alias("total_price_cume_dist_cume_dist_2_cume_dist"),
        F.percentile_approx("price_cume_dist_cume_dist", 0.9).alias("price_cume_dist_cume_dist_p90"),
        F.count("*").alias("event_count"),
        F.percentile_approx("price_cume_dist_dense_rank", 0.9).alias("price_cume_dist_dense_rank_p90"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("price_cume_dist_cume_dist_p90") > 0)
)
