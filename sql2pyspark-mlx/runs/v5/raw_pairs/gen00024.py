from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_first())
w2 = Window.orderBy(F.col("val").asc_nulls_first())
w3 = Window.orderBy(F.col("val_cume_dist").desc_nulls_last())
w4 = Window.orderBy(F.col("val_cume_dist").desc_nulls_first())
w5 = Window.orderBy(F.col("val_cume_dist_percent_rank").desc_nulls_last())
w6 = Window.orderBy(F.col("val_cume_dist_dense_rank").desc_nulls_first())
w7 = Window.orderBy(F.col("val_cume_dist_percent_rank").desc())
w8 = Window.orderBy(F.col("val_rounded").asc_nulls_last())
w_hist = Window.orderBy("val_cume_dist_percent_rank")

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("val"))
)

metrics = flattened.select("id", "val", F.cume_dist().over(w_recent).alias("val_cume_dist"))

scored = (
    metrics
    .filter(F.col("val") == 40)
    .select(
        "id",
        "val",
        F.dense_rank().over(w2).alias("val_dense_rank"),
        F.dense_rank().over(w3).alias("val_cume_dist_dense_rank"),
        F.percent_rank().over(w4).alias("val_cume_dist_percent_rank"),
    )
)

scored_price_tiers = (
    scored
    .select(
        "id",
        "val",
        "val_dense_rank",
        "val_cume_dist_percent_rank",
        F.cume_dist().over(w5).alias("val_cume_dist_percent_rank_cume_dist"),
        F.cume_dist().over(w6).alias("val_cume_dist_dense_rank_cume_dist"),
        F.percent_rank().over(w7).alias("val_cume_dist_percent_rank_percent_rank"),
    )
)

filtered = (
    scored_price_tiers
    .filter(F.col("val_cume_dist_percent_rank") > 0.64)
    .withColumn("val_cume_dist_percent_rank_plus_val_cume_dist_percent_rank_cume_dist", (F.col("val_cume_dist_percent_rank") + F.col("val_cume_dist_percent_rank_cume_dist")))
)

filtered_price_tiers = (
    filtered
    .filter(F.col("val_dense_rank").isNotNull())
    .select("*", F.round("val", 0).alias("val_rounded"))
)

with_windows = (
    filtered_price_tiers
    .select(
        "val",
        "val_cume_dist_percent_rank",
        "val_cume_dist_dense_rank_cume_dist",
        "val_cume_dist_percent_rank_percent_rank",
        "val_cume_dist_percent_rank_plus_val_cume_dist_percent_rank_cume_dist",
        F.dense_rank().over(w8).alias("val_rounded_dense_rank"),
        F.dense_rank().over(w_hist).alias("val_cume_dist_percent_rank_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "val",
        "val_cume_dist_dense_rank_cume_dist",
        "val_cume_dist_percent_rank_percent_rank",
        "val_cume_dist_percent_rank_plus_val_cume_dist_percent_rank_cume_dist",
        "val_rounded_dense_rank",
        "val_cume_dist_percent_rank_dense_rank",
    )
    .orderBy(F.col("val").asc_nulls_first())
)
