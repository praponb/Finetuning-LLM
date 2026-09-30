from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").asc_nulls_first())
w2 = Window.orderBy(F.col("val").desc())
w3 = Window.orderBy(F.col("val").desc_nulls_last())
w4 = Window.orderBy("val_cume_dist")
w5 = Window.orderBy("val_rank")
w6 = Window.orderBy(F.col("val_rank").asc_nulls_last())
w7 = Window.orderBy(F.col("val_cume_dist_cume_dist").desc_nulls_last())
w_grp = Window.orderBy(F.col("val_cume_dist_cume_dist").asc_nulls_last())
w9 = Window.orderBy(F.col("val_cume_dist_cume_dist_percent_rank").desc_nulls_last())

exploded = df_price_tiers.select("id", "effective_from", F.explode("discounts").alias("val"))

scored = (
    exploded
    .filter(F.col("effective_from").between("2024-04-29", "2025-08-01"))
    .select(
        "*",
        F.rank().over(w).alias("val_rank"),
        F.cume_dist().over(w2).alias("val_cume_dist"),
        F.dense_rank().over(w3).alias("val_dense_rank"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "effective_from",
        "val_rank",
        F.cume_dist().over(w4).alias("val_cume_dist_cume_dist"),
    )
)

with_windows = (
    enriched
    .select(
        "effective_from",
        "val_rank",
        F.dense_rank().over(w5).alias("val_rank_dense_rank"),
        F.cume_dist().over(w6).alias("val_rank_cume_dist"),
        F.rank().over(w7).alias("val_cume_dist_cume_dist_rank"),
        F.percent_rank().over(w_grp).alias("val_cume_dist_cume_dist_percent_rank"),
    )
)

scored_price_tiers = (
    with_windows
    .withColumn("val_rank_cume_dist_2", F.cume_dist().over(w5))
    .withColumn("val_cume_dist_cume_dist_percent_rank_percent_rank", F.percent_rank().over(w9))
)

result = (
    scored_price_tiers
    .select(
        "effective_from",
        "val_rank",
        "val_rank_cume_dist",
        "val_cume_dist_cume_dist_rank",
        "val_rank_cume_dist_2",
        "val_cume_dist_cume_dist_percent_rank_percent_rank",
    )
    .orderBy(F.col("val_rank").desc_nulls_last())
)
