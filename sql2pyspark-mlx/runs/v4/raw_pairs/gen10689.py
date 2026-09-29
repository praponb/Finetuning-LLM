from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("currency")
w_grp_2 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_last())
w_grp_3 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("val"),
    )
)

with_windows = (
    lines
    .select(
        "*",
        F.stddev_samp("val").over(w_grp).alias("currency_stddev_samp_val"),
        F.max("val").over(w_grp).alias("currency_max_val"),
        F.rank().over(w_grp_2).alias("val_rank"),
        F.dense_rank().over(w_grp_3).alias("val_dense_rank"),
    )
)

valid = (
    with_windows
    .filter((F.col("effective_from") > F.lit("2024-03-02").cast("date")) & F.col("val_rank").isNull())
    .select(
        "*",
        F.greatest("currency_stddev_samp_val", "val_dense_rank").alias("greatest_currency_stddev_samp_val_val_dense_rank"),
    )
)

cleaned = (
    valid
    .filter((F.col("effective_from") >= F.to_date(F.lit("2024-04-01"))) & (F.col("currency_stddev_samp_val") == 66))
    .select(
        "effective_from",
        "val",
        "currency_stddev_samp_val",
        "currency_max_val",
        "val_rank",
        "val_dense_rank",
        F.coalesce("val_dense_rank", F.lit(0)).alias("val_dense_rank_filled"),
        F.date_format("effective_from", "yyyy-MM").alias("effective_from_label"),
    )
)

filtered = (
    cleaned
    .filter(F.col("effective_from") >= "2025-01-01")
    .select(
        "effective_from",
        "val",
        "currency_stddev_samp_val",
        "currency_max_val",
        "val_rank",
        "val_dense_rank_filled",
        "effective_from_label",
        (F.col("val") * F.col("currency_max_val")).alias("val_times_currency_max_val"),
        F.floor("val").alias("val_whole"),
    )
)

totals = (
    filtered
    .groupBy(
        F.when(F.col("val_dense_rank_filled") >= 67, "PREMIUM").otherwise("STANDARD").alias("val_dense_rank_filled_tier"),
    )
    .agg(
        F.percentile_approx("val", 0.5).alias("val_p50"),
        F.percentile_approx("currency_stddev_samp_val", 0.9).alias("currency_stddev_samp_val_p90"),
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles"),
        F.round(F.avg("currency_stddev_samp_val"), 2).alias("avg_currency_stddev_samp_val"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("currency_stddev_samp_val_p90") >= 0)
)

result = (
    totals
    .select(
        "val_dense_rank_filled_tier",
        "currency_stddev_samp_val_p90",
        "val_quartiles",
        "avg_currency_stddev_samp_val",
        "num_rows",
    )
    .orderBy(F.col("avg_currency_stddev_samp_val").asc_nulls_first())
)
