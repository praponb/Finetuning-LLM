from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").asc_nulls_last())
w_part = Window.orderBy(F.col("val").desc())
w_part_2 = Window.orderBy(F.col("val").desc_nulls_first())
w4 = Window.orderBy(F.col("val_rank_2").asc_nulls_last())
w5 = Window.orderBy(F.col("val_rank_2_times_val_rank").desc_nulls_last())
w6 = Window.orderBy("val_rank_2_rounded")
w7 = Window.orderBy(F.col("val_rank").asc_nulls_last())

flattened = df_price_tiers.select("id", "effective_from", F.explode("tier_limits").alias("val"))

enriched = (
    flattened
    .withColumn("val_rank", F.rank().over(w))
    .withColumn("val_rank_2", F.rank().over(w_part))
    .withColumn("val_rank_3", F.rank().over(w_part_2))
    .withColumn("val_rank_4", F.rank().over(w_part))
    .withColumn("val_percent_rank", F.percent_rank().over(w_part_2))
)

cleaned = (
    enriched
    .filter(F.col("val") != 69)
    .withColumn("val_rank_2_times_val_rank", (F.col("val_rank_2") * F.col("val_rank")))
    .withColumn("val_rank_2_rounded", F.round("val_rank_2", 2))
)

scored = (
    cleaned
    .filter(F.col("val_rank_2_times_val_rank").between(37, 57))
    .select(
        "id",
        "val_rank",
        "val_rank_2",
        "val_rank_3",
        "val_percent_rank",
        "val_rank_2_times_val_rank",
        "val_rank_2_rounded",
        F.rank().over(w4).alias("val_rank_2_rank"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "val_rank_3",
        "val_percent_rank",
        "val_rank_2_times_val_rank",
        "val_rank_2_rank",
        F.dense_rank().over(w5).alias("val_rank_2_times_val_rank_dense_rank"),
        F.cume_dist().over(w6).alias("val_rank_2_rounded_cume_dist"),
        F.cume_dist().over(w7).alias("val_rank_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "id",
        "val_rank_3",
        "val_percent_rank",
        "val_rank_2_times_val_rank",
        "val_rank_2_rank",
        "val_rank_2_times_val_rank_dense_rank",
        "val_rank_cume_dist",
    )
    .orderBy(F.col("val_rank_3").desc())
)
