from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("currency_stddev_samp_tier_value").asc_nulls_first())
w3 = Window.orderBy(F.col("currency_max_currency_min_tier_value").asc_nulls_first())
w4 = Window.orderBy("currency_min_tier_value_per_currency_stddev_samp_tier_value")

lines = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_prices").alias("tier_value"))
)

metrics = (
    lines
    .select("*", F.min("tier_value").over(w_recent).alias("currency_min_tier_value"))
)

scored = (
    metrics
    .select(
        "id",
        "currency_min_tier_value",
        F.stddev_samp("tier_value").over(w_recent).alias("currency_stddev_samp_tier_value"),
        F.max("currency_min_tier_value").over(w_recent).alias("currency_max_currency_min_tier_value"),
    )
)

metrics_price_tiers = scored.withColumn("currency_stddev_samp_tier_value_rank", F.rank().over(w2))

filtered = (
    metrics_price_tiers
    .filter(F.col("currency_stddev_samp_tier_value") > 69)
    .withColumn("currency_stddev_samp_tier_value_rank_times_currency_min_tier_value", (F.col("currency_stddev_samp_tier_value_rank") * F.col("currency_min_tier_value")))
    .withColumn("currency_min_tier_value_per_currency_stddev_samp_tier_value", (F.col("currency_min_tier_value") / F.nullif("currency_stddev_samp_tier_value", F.lit(0))))
)

with_windows = (
    filtered
    .select(
        "currency_min_tier_value",
        "currency_stddev_samp_tier_value",
        "currency_stddev_samp_tier_value_rank",
        F.cume_dist().over(w3).alias("currency_max_currency_min_tier_value_cume_dist"),
        F.dense_rank().over(w4).alias("currency_min_tier_value_per_currency_stddev_samp_tier_value_dense_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("currency_stddev_samp_tier_value") >= 88, "HIGH").otherwise("LOW").alias("currency_stddev_samp_tier_value_tier"),
    )
    .agg(
        F.percentile_approx("currency_stddev_samp_tier_value", [0.25, 0.5, 0.75]).alias("currency_stddev_samp_tier_value_quartiles"),
        F.count("*").alias("row_count"),
        F.round(F.avg("currency_stddev_samp_tier_value"), 2).alias("avg_currency_stddev_samp_tier_value"),
        F.percentile_approx("currency_stddev_samp_tier_value_rank", [0.25, 0.5, 0.75]).alias("currency_stddev_samp_tier_value_rank_quartiles"),
    )
    .orderBy(F.col("avg_currency_stddev_samp_tier_value").asc_nulls_first())
)
