from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value")
w2 = Window.orderBy(F.col("price").desc())
w_recent = Window.orderBy(F.col("price_rank").desc_nulls_first())
w4 = Window.orderBy(F.col("price").desc_nulls_last())
w5 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w6 = Window.orderBy(F.col("price").asc_nulls_last())

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "rec"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        (F.col("pos") + 1).alias("item_no"),
        F.col("rec.qty"),
        F.col("rec.price"),
    )
)

lines = flattened.select("created_at", "price", F.explode("coupon_codes").alias("value"))

scored = (
    lines
    .select(
        "created_at",
        "price",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.rank().over(w2).alias("price_rank"),
    )
)

metrics = (
    scored
    .filter(F.col("price_rank").between(4, 8))
    .select(
        "price",
        "price_share",
        "price_rank",
        F.percent_rank().over(w_recent).alias("price_rank_percent_rank"),
        F.cume_dist().over(w4).alias("price_cume_dist"),
        F.max("price").over(w5).alias("price_max_1h"),
    )
)

with_windows = (
    metrics
    .select(
        "price_share",
        "price_cume_dist",
        F.rank().over(w2).alias("price_rank_2"),
        F.dense_rank().over(w6).alias("price_dense_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("price_cume_dist") >= 0.9, "PREMIUM").otherwise("STANDARD").alias("price_cume_dist_tier"),
    )
    .agg(
        F.round(F.avg("price_dense_rank"), 2).alias("avg_price_dense_rank"),
        F.percentile_approx("price_dense_rank", [0.25, 0.5, 0.75]).alias("price_dense_rank_quartiles"),
        F.round(F.avg("price_dense_rank"), 2).alias("avg_price_dense_rank_2"),
        F.percentile_approx("price_rank_2", [0.25, 0.5, 0.75]).alias("price_rank_2_quartiles"),
    )
)
