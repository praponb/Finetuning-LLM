from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc_nulls_last())
w2 = Window.orderBy(F.col("val").desc())
w_recent = Window.orderBy(F.col("val_percent_rank").desc())
w4 = Window.orderBy(F.col("val_percent_rank_2_times_val_percent_rank_percent_rank").asc_nulls_last())
w5 = Window.orderBy(F.col("val_percent_rank_plus_val").desc_nulls_last())
w6 = Window.orderBy(F.col("val_percent_rank_2_times_val_percent_rank_percent_rank_dense_rank").desc())

exploded = df_price_tiers.select("id", F.explode_outer("tier_limits").alias("val"))

enriched = exploded.select("*", F.percent_rank().over(w).alias("val_percent_rank"))

valid = (
    enriched
    .filter(F.col("val_percent_rank") > 0.42)
    .select(
        "*",
        (F.col("val_percent_rank") + F.col("val")).alias("val_percent_rank_plus_val"),
    )
)

metrics = (
    valid
    .withColumn("val_percent_rank_2", F.percent_rank().over(w2))
    .withColumn("val_percent_rank_percent_rank", F.percent_rank().over(w_recent))
)

filtered = (
    metrics
    .filter(F.col("val_percent_rank") >= 0.58)
    .select(
        "*",
        (F.col("val_percent_rank_2") * F.col("val_percent_rank_percent_rank")).alias("val_percent_rank_2_times_val_percent_rank_percent_rank"),
        (F.col("val_percent_rank_plus_val") - F.col("val_percent_rank_percent_rank")).alias("val_percent_rank_plus_val_minus_val_percent_rank_percent_rank"),
    )
)

scored = (
    filtered
    .filter(F.col("val_percent_rank_2_times_val_percent_rank_percent_rank") >= 25)
    .withColumn("val_percent_rank_2_times_val_percent_rank_percent_rank_dense_rank", F.dense_rank().over(w4))
)

metrics_price_tiers = (
    scored
    .select(
        "val",
        "val_percent_rank_plus_val",
        "val_percent_rank_percent_rank",
        "val_percent_rank_plus_val_minus_val_percent_rank_percent_rank",
        "val_percent_rank_2_times_val_percent_rank_percent_rank_dense_rank",
        F.percent_rank().over(w5).alias("val_percent_rank_plus_val_percent_rank"),
        F.cume_dist().over(w_recent).alias("val_percent_rank_cume_dist"),
        F.cume_dist().over(w6).alias("val_percent_rank_2_times_val_percent_rank_percent_rank_dense_rank_cume_dist"),
    )
)

result = (
    metrics_price_tiers
    .select(
        "val",
        "val_percent_rank_plus_val",
        "val_percent_rank_plus_val_minus_val_percent_rank_percent_rank",
        "val_percent_rank_2_times_val_percent_rank_percent_rank_dense_rank",
        "val_percent_rank_plus_val_percent_rank",
        "val_percent_rank_cume_dist",
        "val_percent_rank_2_times_val_percent_rank_percent_rank_dense_rank_cume_dist",
    )
    .orderBy(
        F.col("val_percent_rank_2_times_val_percent_rank_percent_rank_dense_rank_cume_dist").asc_nulls_last(),
    )
)
