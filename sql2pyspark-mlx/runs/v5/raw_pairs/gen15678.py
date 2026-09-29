from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc())
w2 = Window.orderBy("val")
w3 = Window.orderBy(F.col("val").desc_nulls_last())
w4 = Window.orderBy(F.col("val_cume_dist").desc_nulls_last())
w5 = Window.orderBy(F.col("val_cume_dist").desc_nulls_first())
w6 = Window.orderBy(F.col("val_rank").asc_nulls_first())
w_hist = Window.orderBy(F.col("val_cume_dist").desc())
w_hist_2 = Window.orderBy(F.col("adjusted_val_times_val_rank").desc())

lines = df_price_tiers.select("id", "effective_from", F.explode("tier_limits").alias("val"))

enriched = (
    lines
    .select(
        "id",
        "effective_from",
        "val",
        F.dense_rank().over(w).alias("val_dense_rank"),
        F.cume_dist().over(w2).alias("val_cume_dist"),
        F.rank().over(w3).alias("val_rank"),
    )
)

base = (
    enriched
    .filter((F.col("effective_from") > F.lit("2024-03-10").cast("date")) & (F.col("val_rank") > 6))
    .select(
        "id",
        "effective_from",
        "val_cume_dist",
        "val_rank",
        (F.col("val_rank") + F.col("val")).alias("val_rank_plus_val"),
        (F.col("val") * F.col("val_rank")).alias("val_times_val_rank"),
    )
)

filtered = (
    base
    .filter(F.col("val_rank") < 6)
    .select(
        "id",
        "val_cume_dist",
        "val_rank",
        "val_rank_plus_val",
        (F.col("val_rank") - F.col("val_cume_dist")).alias("val_rank_minus_val_cume_dist"),
        F.round((F.col("val_times_val_rank") * 1.1), 2).alias("adjusted_val_times_val_rank"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "val_cume_dist",
        F.cume_dist().over(w4).alias("val_cume_dist_cume_dist"),
        F.cume_dist().over(w5).alias("val_cume_dist_cume_dist_2"),
        F.percent_rank().over(w6).alias("val_rank_percent_rank"),
        F.percent_rank().over(w_hist).alias("val_cume_dist_percent_rank"),
        F.percent_rank().over(w_hist_2).alias("adjusted_val_times_val_rank_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "val_cume_dist_cume_dist_2",
        "val_cume_dist_percent_rank",
        "adjusted_val_times_val_rank_percent_rank",
    )
    .orderBy(F.col("val_cume_dist_cume_dist_2").asc_nulls_first())
)
