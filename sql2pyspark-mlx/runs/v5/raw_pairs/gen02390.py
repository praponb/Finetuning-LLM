from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("currency_stddev_samp_val").asc_nulls_first())
w3 = Window.orderBy(F.col("currency_stddev_samp_val").desc_nulls_first())
w4 = Window.orderBy(F.col("val").desc())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

scored = lines.withColumn("currency_stddev_samp_val", F.stddev_samp("val").over(w_running))

with_windows = (
    scored
    .select(
        "*",
        F.max("val").over(w_running).alias("currency_max_val"),
        F.percent_rank().over(w2).alias("currency_stddev_samp_val_percent_rank"),
    )
)

filtered = (
    with_windows
    .filter((F.col("effective_from") > F.lit("2024-06-13").cast("date")) & (F.col("currency_max_val") > 46))
    .select(
        "id",
        "effective_from",
        "val",
        "currency_stddev_samp_val",
        "currency_max_val",
        "currency_stddev_samp_val_percent_rank",
        F.upper("currency").alias("currency_upper"),
    )
)

scored_price_tiers = (
    filtered
    .filter(F.col("currency_stddev_samp_val").between(21, 40))
    .select(
        "id",
        "val",
        "currency_max_val",
        "currency_stddev_samp_val_percent_rank",
        "currency_upper",
        F.rank().over(w3).alias("currency_stddev_samp_val_rank"),
        F.percent_rank().over(w4).alias("val_percent_rank"),
    )
)

filtered_price_tiers = (
    scored_price_tiers
    .filter(F.col("currency_stddev_samp_val_rank") >= 4)
    .select(
        "id",
        "val",
        "currency_max_val",
        "currency_upper",
        "currency_stddev_samp_val_rank",
        "val_percent_rank",
        F.when(F.col("currency_stddev_samp_val_rank") >= 8, "High").when(F.col("currency_stddev_samp_val_rank") >= 4, "Medium").otherwise("Low").alias("currency_stddev_samp_val_rank_band"),
    )
)

daily = (
    filtered_price_tiers
    .groupBy(
        F.when(F.col("val_percent_rank") >= 0.86, "PREMIUM").otherwise("STANDARD").alias("val_percent_rank_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("val", 0.95).alias("val_p95"),
        F.count("*").alias("row_count"),
        F.percentile_approx("currency_stddev_samp_val_rank", 0.5).alias("currency_stddev_samp_val_rank_p50"),
        F.count("*").alias("row_count_2"),
        F.percentile_approx("currency_stddev_samp_val_rank", [0.25, 0.5, 0.75]).alias("currency_stddev_samp_val_rank_quartiles"),
    )
)

result = (
    daily
    .select(
        "val_percent_rank_tier",
        "num_rows",
        "row_count",
        "currency_stddev_samp_val_rank_p50",
        "row_count_2",
        "currency_stddev_samp_val_rank_quartiles",
    )
    .orderBy(F.col("row_count").desc_nulls_first())
)
