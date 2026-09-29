from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())
w3 = Window.orderBy(F.col("val").desc_nulls_first())
w4 = Window.orderBy(F.col("currency_max_val").desc())
w5 = Window.orderBy(F.col("val_percent_rank_plus_currency_stddev_samp_val_plus_val_percent_rank").desc_nulls_last())

lines = df_price_tiers.select("id", "currency", F.explode_outer("discounts").alias("val"))

enriched = (
    lines
    .select(
        "id",
        "currency",
        "val",
        F.max("val").over(w).alias("currency_max_val"),
        F.percent_rank().over(w2).alias("val_percent_rank"),
        F.stddev_samp("val").over(w).alias("currency_stddev_samp_val"),
        F.count("val").over(w).alias("currency_count_val"),
        F.cume_dist().over(w3).alias("val_cume_dist"),
    )
)

filtered = (
    enriched
    .filter(F.col("val_percent_rank") >= 0.28)
    .select(
        "*",
        (F.col("val_percent_rank") + F.col("currency_stddev_samp_val")).alias("val_percent_rank_plus_currency_stddev_samp_val"),
    )
)

base = (
    filtered
    .filter(F.col("val_percent_rank") < 0.35)
    .select(
        "id",
        "currency",
        "currency_max_val",
        "val_percent_rank",
        "currency_count_val",
        "val_cume_dist",
        F.when(F.col("currency").isin(["EUR", "THB"]), "Tier 1").otherwise("Other").alias("currency_group"),
        (F.col("val_percent_rank_plus_currency_stddev_samp_val") + F.col("val_percent_rank")).alias("val_percent_rank_plus_currency_stddev_samp_val_plus_val_percent_rank"),
    )
)

enriched_price_tiers = (
    base
    .filter(F.col("val_cume_dist").between(0.56, 0.79))
    .select(
        "currency_max_val",
        "val_percent_rank_plus_currency_stddev_samp_val_plus_val_percent_rank",
        F.avg("val_percent_rank_plus_currency_stddev_samp_val_plus_val_percent_rank").over(w).alias("currency_avg_val_percent_rank_plus_currency_stddev_samp_val_plus_val_percent_rank"),
    )
)

result = (
    enriched_price_tiers
    .select(
        "val_percent_rank_plus_currency_stddev_samp_val_plus_val_percent_rank",
        "currency_avg_val_percent_rank_plus_currency_stddev_samp_val_plus_val_percent_rank",
        F.cume_dist().over(w4).alias("currency_max_val_cume_dist"),
        F.dense_rank().over(w5).alias("val_percent_rank_plus_currency_stddev_samp_val_plus_val_percent_rank_dense_rank"),
    )
)
