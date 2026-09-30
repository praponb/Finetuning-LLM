from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())
w3 = Window.partitionBy("currency").orderBy(F.col("currency_count_val").desc_nulls_first())
w4 = Window.partitionBy("currency").orderBy(F.col("currency_max_val").desc_nulls_first())
w5 = Window.partitionBy("currency").orderBy(F.col("currency_max_val_dense_rank").asc_nulls_first())
w6 = Window.partitionBy("currency").orderBy(F.col("currency_stddev_samp_currency_max_val_dense_rank").desc_nulls_first())
w7 = Window.orderBy(F.col("val").desc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("tier_prices").alias("val"))
)

scored = (
    lines
    .withColumn("currency_stddev_samp_val", F.stddev_samp("val").over(w))
    .withColumn("currency_max_val", F.max("val").over(w))
    .withColumn("currency_count_val", F.count("val").over(w))
    .withColumn("val_dense_rank", F.dense_rank().over(w2))
)

metrics = (
    scored
    .select(
        "*",
        F.avg("currency_stddev_samp_val").over(w).alias("currency_avg_currency_stddev_samp_val"),
        F.rank().over(w3).alias("currency_count_val_rank"),
        F.dense_rank().over(w4).alias("currency_max_val_dense_rank"),
    )
)

metrics_price_tiers = (
    metrics
    .withColumn("currency_max_val_dense_rank_percent_rank", F.percent_rank().over(w5))
    .withColumn("currency_stddev_samp_currency_max_val_dense_rank", F.stddev_samp("currency_max_val_dense_rank").over(w))
)

with_windows = (
    metrics_price_tiers
    .select(
        "*",
        F.dense_rank().over(w6).alias("currency_stddev_samp_currency_max_val_dense_rank_dense_rank"),
        F.sum("currency_avg_currency_stddev_samp_val").over(w).alias("currency_sum_currency_avg_currency_stddev_samp_val"),
        F.percent_rank().over(w7).alias("val_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "effective_from",
        "currency",
        "val",
        "currency_stddev_samp_val",
        "currency_count_val",
        "currency_avg_currency_stddev_samp_val",
        "currency_count_val_rank",
        "currency_max_val_dense_rank",
    )
    .orderBy(F.col("currency_count_val").asc_nulls_first())
)
