from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("val").desc_nulls_last())
w3 = Window.partitionBy("plan").orderBy("currency_max_val")
w4 = Window.orderBy(F.col("currency_max_val_dense_rank").asc_nulls_last())
w5 = Window.partitionBy("plan")
w6 = Window.orderBy(F.col("currency_stddev_samp_val_cume_dist").desc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("val"))
)

scored = (
    lines
    .filter(F.col("val") < 71)
    .select(
        "plan",
        "currency",
        F.max("val").over(w).alias("currency_max_val"),
        F.cume_dist().over(w2).alias("val_cume_dist"),
    )
)

cleaned = (
    scored
    .filter(F.col("plan") == "basic")
    .select("*", F.substring("plan", 1, 4).alias("plan_prefix"))
)

scored_price_tiers = (
    cleaned
    .select(
        "plan",
        "currency_max_val",
        "plan_prefix",
        F.dense_rank().over(w3).alias("currency_max_val_dense_rank"),
        F.stddev_samp("val_cume_dist").over(w).alias("currency_stddev_samp_val_cume_dist"),
    )
)

enriched = (
    scored_price_tiers
    .select(
        "*",
        F.dense_rank().over(w4).alias("currency_max_val_dense_rank_dense_rank"),
        F.stddev_samp("currency_stddev_samp_val_cume_dist").over(w5).alias("plan_stddev_samp_currency_stddev_samp_val_cume_dist"),
    )
)

with_windows = (
    enriched
    .select(
        "currency_max_val",
        "currency_max_val_dense_rank",
        "currency_stddev_samp_val_cume_dist",
        "plan_stddev_samp_currency_stddev_samp_val_cume_dist",
        F.percent_rank().over(w6).alias("currency_stddev_samp_val_cume_dist_percent_rank"),
        F.sum("currency_max_val").over(w5).alias("plan_sum_currency_max_val"),
    )
)

result = (
    with_windows
    .rollup(
        F.when(F.col("plan_sum_currency_max_val") >= 341, "LARGE").otherwise("SMALL").alias("plan_sum_currency_max_val_tier"),
        F.when(F.col("currency_stddev_samp_val_cume_dist") >= 1, "LARGE").otherwise("SMALL").alias("currency_stddev_samp_val_cume_dist_tier"),
    )
    .agg(
        F.round(F.avg("currency_stddev_samp_val_cume_dist"), 2).alias("avg_currency_stddev_samp_val_cume_dist"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("currency_max_val_dense_rank", [0.25, 0.5, 0.75]).alias("currency_max_val_dense_rank_quartiles"),
        F.round(F.avg("currency_max_val_dense_rank"), 2).alias("avg_currency_max_val_dense_rank"),
    )
)
