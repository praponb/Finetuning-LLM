from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())
w_running = Window.partitionBy("currency")
w3 = Window.orderBy(F.col("currency_stddev_samp_val").asc_nulls_last())
w4 = Window.partitionBy("currency").orderBy(F.col("val_cume_dist").desc_nulls_first())
w5 = Window.partitionBy("currency").orderBy(F.col("currency_min_currency_min_val").desc_nulls_first())

flattened = df_price_tiers.select("id", "currency", F.explode("tier_limits").alias("val"))

with_windows = flattened.select("*", F.cume_dist().over(w).alias("val_cume_dist"))

valid = (
    with_windows
    .filter(F.col("currency").isin(["THB", "USD"]))
    .withColumn("val_cume_dist_level", F.when(F.col("val_cume_dist") >= 0.8, "High").when(F.col("val_cume_dist") >= 0.19, "Medium").otherwise("Low"))
)

enriched = (
    valid
    .withColumn("currency_stddev_samp_val", F.stddev_samp("val").over(w_running))
    .withColumn("currency_min_val", F.min("val").over(w_running))
)

enriched_price_tiers = (
    enriched
    .withColumn("currency_count_val_cume_dist", F.count("val_cume_dist").over(w_running))
    .withColumn("currency_max_val_cume_dist", F.max("val_cume_dist").over(w_running))
    .withColumn("currency_min_currency_min_val", F.min("currency_min_val").over(w_running))
    .withColumn("currency_stddev_samp_val_cume_dist", F.cume_dist().over(w3))
)

scored = (
    enriched_price_tiers
    .select(
        "val_cume_dist_level",
        "currency_stddev_samp_val",
        "currency_min_val",
        "currency_count_val_cume_dist",
        "currency_max_val_cume_dist",
        "currency_min_currency_min_val",
        F.dense_rank().over(w4).alias("val_cume_dist_dense_rank"),
        F.cume_dist().over(w5).alias("currency_min_currency_min_val_cume_dist"),
        F.avg("val_cume_dist").over(w_running).alias("currency_avg_val_cume_dist"),
        F.avg("currency_stddev_samp_val").over(w_running).alias("currency_avg_currency_stddev_samp_val"),
    )
)

filtered = (
    scored
    .filter(F.col("currency_min_currency_min_val_cume_dist") >= 0.29)
    .select(
        "*",
        (F.col("currency_max_val_cume_dist") + F.col("currency_min_currency_min_val_cume_dist")).alias("currency_max_val_cume_dist_plus_currency_min_currency_min_val_cume_dist"),
        F.coalesce("currency_stddev_samp_val", F.lit(0)).alias("currency_stddev_samp_val_filled"),
    )
)

result = (
    filtered
    .rollup(
        F.when(F.col("currency_min_currency_min_val_cume_dist") >= 0.77, "HIGH").otherwise("LOW").alias("currency_min_currency_min_val_cume_dist_tier"),
        F.when(F.col("currency_stddev_samp_val_filled") >= 77, "PREMIUM").otherwise("STANDARD").alias("currency_stddev_samp_val_filled_tier"),
    )
    .agg(
        F.percentile_approx("currency_avg_val_cume_dist", [0.25, 0.5, 0.75]).alias("currency_avg_val_cume_dist_quartiles"),
        F.percentile_approx("val_cume_dist_dense_rank", [0.25, 0.5, 0.75]).alias("val_cume_dist_dense_rank_quartiles"),
        F.avg("currency_max_val_cume_dist").alias("avg_currency_max_val_cume_dist"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") >= 1)
)
