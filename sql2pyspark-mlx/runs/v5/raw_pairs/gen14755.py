from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").asc_nulls_first())
w2 = Window.partitionBy("currency").orderBy(F.col("val_cume_dist").desc_nulls_first())
w3 = Window.partitionBy("currency").orderBy(F.col("val_cume_dist").desc())
w4 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_last())
w5 = Window.partitionBy("currency")
w_running = Window.partitionBy("currency").orderBy(F.col("currency_min_val").desc_nulls_last())

lines = df_price_tiers.select("id", "currency", F.explode_outer("tier_limits").alias("val"))

enriched = lines.select("*", F.cume_dist().over(w).alias("val_cume_dist"))

metrics = (
    enriched
    .select(
        "id",
        "currency",
        "val",
        F.dense_rank().over(w2).alias("val_cume_dist_dense_rank"),
        F.percent_rank().over(w3).alias("val_cume_dist_percent_rank"),
        F.cume_dist().over(w4).alias("val_cume_dist_2"),
        F.min("val").over(w5).alias("currency_min_val"),
        F.avg("val_cume_dist").over(w5).alias("currency_avg_val_cume_dist"),
    )
)

with_windows = (
    metrics
    .filter(F.col("val_cume_dist_2") >= 0.58)
    .select(
        "*",
        F.sum("currency_avg_val_cume_dist").over(w5).alias("currency_sum_currency_avg_val_cume_dist"),
        F.percent_rank().over(w_running).alias("currency_min_val_percent_rank"),
        F.rank().over(w_running).alias("currency_min_val_rank"),
    )
)

valid = (
    with_windows
    .filter(F.col("val_cume_dist_percent_rank") <= 0.3)
    .select(
        "id",
        "val_cume_dist_dense_rank",
        "val_cume_dist_percent_rank",
        "val_cume_dist_2",
        "currency_avg_val_cume_dist",
        "currency_sum_currency_avg_val_cume_dist",
        "currency_min_val_percent_rank",
        "currency_min_val_rank",
        F.greatest("currency_min_val_rank", "val_cume_dist_percent_rank").alias("greatest_currency_min_val_rank_val_cume_dist_percent_rank"),
        F.col("val_cume_dist_2").cast("int").alias("val_cume_dist_2_int"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("currency_min_val_rank") >= 6, "HIGH").otherwise("LOW").alias("currency_min_val_rank_tier"),
    )
    .agg(
        F.max("val_cume_dist_2_int").alias("max_val_cume_dist_2_int"),
        F.min("val_cume_dist_percent_rank").alias("min_val_cume_dist_percent_rank"),
        F.percentile_approx("val_cume_dist_2_int", [0.25, 0.5, 0.75]).alias("val_cume_dist_2_int_quartiles"),
    )
    .filter(F.col("min_val_cume_dist_percent_rank") > 0)
    .orderBy(F.col("max_val_cume_dist_2_int").desc())
)
