from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("element").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("element")
w3 = Window.orderBy("qty")
w4 = Window.partitionBy("element").orderBy(F.col("element_count_qty").desc_nulls_first())
w5 = Window.partitionBy("element").orderBy(F.col("qty_rank").asc_nulls_first())
w6 = Window.partitionBy("element").orderBy(F.col("qty_rank").asc_nulls_last())
w7 = Window.partitionBy("element").orderBy(F.col("qty_share").desc_nulls_last())
w8 = Window.orderBy(F.col("element_count_qty_percent_rank").asc_nulls_last())
w9 = Window.partitionBy("element").orderBy(F.col("qty_rank_percent_rank").desc_nulls_first())
w10 = Window.orderBy(F.col("qty").desc_nulls_first())
w11 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w12 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_ordered = Window.orderBy(F.col("qty_share_plus_qty_rank").desc_nulls_first())

flattened = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
    )
)

lines = flattened.select("created_at", "qty", F.explode("coupon_codes").alias("element"))

scored = (
    lines
    .withColumn("qty_sum_1h", F.sum("qty").over(w_hist))
    .withColumn("qty_share", F.round((F.col("qty") / F.sum("qty").over(w2)), 4))
    .withColumn("element_count_qty", F.count("qty").over(w2))
    .withColumn("qty_rank", F.rank().over(w3))
)

with_windows = (
    scored
    .withColumn("element_count_qty_share", F.round((F.col("element_count_qty") / F.sum("element_count_qty").over(w2)), 4))
    .withColumn("element_count_qty_percent_rank", F.percent_rank().over(w4))
    .withColumn("qty_rank_percent_rank", F.percent_rank().over(w5))
    .withColumn("qty_rank_dense_rank", F.dense_rank().over(w6))
    .withColumn("qty_share_percent_rank", F.percent_rank().over(w7))
)

metrics = (
    with_windows
    .select(
        "*",
        F.rank().over(w8).alias("element_count_qty_percent_rank_rank"),
        F.percent_rank().over(w9).alias("qty_rank_percent_rank_percent_rank"),
        F.cume_dist().over(w10).alias("qty_cume_dist"),
    )
)

filtered = (
    metrics
    .filter(F.col("qty_rank_dense_rank") < 7)
    .select(
        "created_at",
        "qty",
        "qty_sum_1h",
        "qty_share",
        "qty_rank",
        "element_count_qty_share",
        "element_count_qty_percent_rank",
        "qty_rank_percent_rank",
        "qty_rank_dense_rank",
        "qty_share_percent_rank",
        "qty_rank_percent_rank_percent_rank",
        "qty_cume_dist",
        (F.col("qty_share") + F.col("qty_rank")).alias("qty_share_plus_qty_rank"),
        F.least("qty_cume_dist", "qty_share_percent_rank").alias("least_qty_cume_dist_qty_share_percent_rank"),
    )
)

with_windows_carts = (
    filtered
    .select(
        "*",
        F.max("qty_cume_dist").over(w11).alias("qty_cume_dist_max_30m"),
        F.avg("qty_rank_dense_rank").over(w12).alias("qty_rank_dense_rank_avg_15m"),
        F.percent_rank().over(w_ordered).alias("qty_share_plus_qty_rank_percent_rank"),
    )
)

result = (
    with_windows_carts
    .select(
        "created_at",
        "qty",
        "qty_sum_1h",
        "qty_share",
        "element_count_qty_share",
        "element_count_qty_percent_rank",
        "qty_rank_dense_rank",
        "qty_share_percent_rank",
        "qty_rank_percent_rank_percent_rank",
    )
    .orderBy(F.col("element_count_qty_percent_rank").desc(), "qty_sum_1h")
)
