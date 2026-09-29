from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("val_filled").desc())
w_running = Window.orderBy(F.col("currency_avg_val").desc_nulls_last())
w4 = Window.partitionBy("currency").orderBy(F.col("val_filled").asc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

valid = (
    lines
    .filter((F.col("effective_from") >= F.to_date(F.lit("2024-09-30"))) & F.col("currency").isin(["THB", "USD", "EUR"]))
    .select("*", F.lower("currency").alias("currency_lower"))
)

base = (
    valid
    .filter((F.col("effective_from") >= F.lit("2024-03-31").cast("date")) & F.col("currency").isin(["EUR", "THB", "USD"]))
    .select(
        "id",
        "effective_from",
        "currency",
        "val",
        "currency_lower",
        F.datediff(F.current_date(), "effective_from").alias("days_since_effective_from"),
        F.coalesce("val", F.lit(0)).alias("val_filled"),
    )
)

with_windows = (
    base
    .withColumn("currency_sum_days_since_effective_from", F.sum("days_since_effective_from").over(w))
    .withColumn("currency_avg_val", F.avg("val").over(w))
    .withColumn("val_filled_dense_rank", F.dense_rank().over(w2))
)

with_windows_price_tiers = (
    with_windows
    .select(
        "*",
        F.cume_dist().over(w_running).alias("currency_avg_val_cume_dist"),
        F.sum("days_since_effective_from").over(w).alias("currency_sum_days_since_effective_from_2"),
        F.percent_rank().over(w4).alias("val_filled_percent_rank"),
    )
)

valid_price_tiers = (
    with_windows_price_tiers
    .filter(F.col("effective_from") >= F.lit("2024-02-11").cast("date"))
    .withColumn("val_filled_plus_currency_sum_days_since_effective_from_2", (F.col("val_filled") + F.col("currency_sum_days_since_effective_from_2")))
)

result = (
    valid_price_tiers
    .groupBy(F.date_trunc("WEEK", "effective_from").alias("week_start"))
    .agg(
        F.percentile_approx("val_filled", [0.25, 0.5, 0.75]).alias("val_filled_quartiles"),
        F.percentile_approx("currency_avg_val", [0.25, 0.5, 0.75]).alias("currency_avg_val_quartiles"),
    )
    .orderBy(F.col("week_start").asc_nulls_last())
)
