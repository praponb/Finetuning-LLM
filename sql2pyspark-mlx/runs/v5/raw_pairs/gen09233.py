from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("tier_value")
w_running = Window.partitionBy("currency")
w_running_2 = Window.orderBy(F.col("tier_value").asc_nulls_last())
w4 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w5 = Window.partitionBy("currency").orderBy(F.col("currency_stddev_samp_tier_value_rounded").asc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

enriched = (
    lines
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w))
    .withColumn("currency_stddev_samp_tier_value", F.stddev_samp("tier_value").over(w_running))
    .withColumn("currency_min_tier_value", F.min("tier_value").over(w_running))
    .withColumn("tier_value_dense_rank_2", F.dense_rank().over(w_running_2))
    .withColumn("currency_avg_tier_value", F.avg("tier_value").over(w_running))
)

filtered = (
    enriched
    .filter(F.col("currency_min_tier_value") >= 59)
    .select(
        "id",
        "currency",
        "tier_value",
        "tier_value_dense_rank",
        "currency_stddev_samp_tier_value",
        "tier_value_dense_rank_2",
        F.round("currency_stddev_samp_tier_value", 0).alias("currency_stddev_samp_tier_value_rounded"),
    )
)

cleaned = (
    filtered
    .filter(F.col("currency").isin(["THB", "EUR", "USD"]))
    .select(
        "*",
        F.coalesce("tier_value_dense_rank", F.lit(0)).alias("tier_value_dense_rank_filled"),
        F.substring("currency", 1, 3).alias("currency_prefix"),
    )
)

metrics = (
    cleaned
    .filter(F.col("currency_stddev_samp_tier_value") != 22)
    .select(
        "id",
        "currency",
        "currency_stddev_samp_tier_value",
        "tier_value_dense_rank_2",
        "tier_value_dense_rank_filled",
        "currency_prefix",
        F.cume_dist().over(w4).alias("tier_value_cume_dist"),
        F.min("tier_value").over(w_running).alias("currency_min_tier_value"),
        F.stddev_samp("currency_stddev_samp_tier_value").over(w_running).alias("currency_stddev_samp_currency_stddev_samp_tier_value"),
        F.dense_rank().over(w5).alias("currency_stddev_samp_tier_value_rounded_dense_rank"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("tier_value_dense_rank_filled") >= 80, "LARGE").otherwise("SMALL").alias("tier_value_dense_rank_filled_tier"),
        "currency",
    )
    .agg(
        F.min("tier_value_cume_dist").alias("min_tier_value_cume_dist"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_count"),
        F.count("*").alias("event_count"),
        F.max(F.when(F.col("currency") == "THB", F.col("currency_stddev_samp_tier_value_rounded_dense_rank"))).alias("thb_max_currency_stddev_samp_tier_value_rounded_dense_rank"),
    )
    .filter(F.col("thb_count") > 1)
)
