from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("is_eur").desc())
w_hist = Window.partitionBy("currency").orderBy("val")
w4 = Window.partitionBy("currency").orderBy(F.col("is_eur_cume_dist").desc_nulls_first())
w5 = Window.partitionBy("currency").orderBy(F.col("is_eur_cume_dist_percent_rank").desc())
w6 = Window.partitionBy("currency").orderBy("currency_stddev_samp_is_eur")
w7 = Window.partitionBy("currency").orderBy(F.col("val_percent_rank").asc_nulls_first())
w8 = Window.orderBy(F.col("currency_stddev_samp_is_eur").desc_nulls_last())
w9 = Window.orderBy(F.col("currency_avg_is_eur_cume_dist_percent_rank").desc_nulls_first())
w10 = Window.orderBy(F.col("is_eur_cume_dist_percent_rank_cume_dist").desc())
w_grp_2 = Window.orderBy(F.col("currency_stddev_samp_is_eur_cume_dist_2").desc())

exploded = df_price_tiers.select("id", "currency", F.explode("tier_limits").alias("val"))

cleaned = (
    exploded
    .filter(F.col("currency") == "THB")
    .select(
        "*",
        F.when(F.col("currency") == "EUR", 1).otherwise(0).alias("is_eur"),
        F.upper("currency").alias("currency_upper"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.sum("is_eur").over(w_running).alias("currency_sum_is_eur"),
        F.cume_dist().over(w2).alias("is_eur_cume_dist"),
        F.stddev_samp("val").over(w_running).alias("currency_stddev_samp_val"),
        F.min("val").over(w_running).alias("currency_min_val"),
        F.percent_rank().over(w_hist).alias("val_percent_rank"),
    )
)

enriched = (
    scored
    .filter(F.col("currency_sum_is_eur") == 181)
    .select(
        "currency",
        "currency_stddev_samp_val",
        "currency_min_val",
        "val_percent_rank",
        F.percent_rank().over(w4).alias("is_eur_cume_dist_percent_rank"),
        F.stddev_samp("is_eur").over(w_running).alias("currency_stddev_samp_is_eur"),
    )
)

enriched_price_tiers = (
    enriched
    .select(
        "*",
        F.max("val_percent_rank").over(w_running).alias("currency_max_val_percent_rank"),
        F.avg("is_eur_cume_dist_percent_rank").over(w_running).alias("currency_avg_is_eur_cume_dist_percent_rank"),
        F.cume_dist().over(w5).alias("is_eur_cume_dist_percent_rank_cume_dist"),
        F.cume_dist().over(w6).alias("currency_stddev_samp_is_eur_cume_dist"),
        F.percent_rank().over(w7).alias("val_percent_rank_percent_rank"),
    )
)

enriched_price_tiers_2 = (
    enriched_price_tiers
    .select(
        "currency",
        "currency_min_val",
        "currency_max_val_percent_rank",
        "is_eur_cume_dist_percent_rank_cume_dist",
        F.cume_dist().over(w8).alias("currency_stddev_samp_is_eur_cume_dist_2"),
        F.dense_rank().over(w9).alias("currency_avg_is_eur_cume_dist_percent_rank_dense_rank"),
    )
)

with_windows = (
    enriched_price_tiers_2
    .withColumn("currency_avg_currency_stddev_samp_is_eur_cume_dist_2", F.avg("currency_stddev_samp_is_eur_cume_dist_2").over(w_running))
    .withColumn("currency_max_is_eur_cume_dist_percent_rank_cume_dist", F.max("is_eur_cume_dist_percent_rank_cume_dist").over(w_running))
    .withColumn("is_eur_cume_dist_percent_rank_cume_dist_dense_rank", F.dense_rank().over(w10))
    .withColumn("currency_min_is_eur_cume_dist_percent_rank_cume_dist", F.min("is_eur_cume_dist_percent_rank_cume_dist").over(w_running))
    .withColumn("currency_stddev_samp_is_eur_cume_dist_2_dense_rank", F.dense_rank().over(w_grp_2))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("currency_min_val") >= 69, "HIGH").otherwise("LOW").alias("currency_min_val_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_count"),
        F.round(F.sum("currency_stddev_samp_is_eur_cume_dist_2_dense_rank") / F.nullif(F.count(F.when(F.col("currency") == "THB", 1)), F.lit(0)), 2).alias("currency_stddev_samp_is_eur_cume_dist_2_dense_rank_per_thb"),
    )
    .orderBy(F.col("currency_min_val_tier").desc_nulls_first())
)
