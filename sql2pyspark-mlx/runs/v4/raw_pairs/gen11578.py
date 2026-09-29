from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("tier_value")
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_first())
w_running = Window.partitionBy("currency")
w5 = Window.partitionBy("currency").orderBy(F.col("tier_value_percent_rank").asc_nulls_last())
w6 = Window.partitionBy("currency").orderBy(F.col("tier_value_dense_rank").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

base = (
    exploded
    .filter(F.col("currency").isin(["EUR", "USD"]))
    .select("*", F.col("tier_value").cast("string").alias("tier_value_num"))
)

cleaned = (
    base
    .filter((F.col("effective_from") > F.lit("2024-08-08").cast("date")) & (F.col("effective_from") >= "2023-08-01"))
    .select(
        "id",
        "currency",
        "tier_value",
        "tier_value_num",
        F.lower("currency").alias("currency_lower"),
        F.lower("currency").alias("currency_lower_2"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.dense_rank().over(w2).alias("tier_value_dense_rank_2"),
        F.rank().over(w3).alias("tier_value_rank"),
    )
)

scored_price_tiers = (
    with_windows
    .select(
        "*",
        F.max("tier_value").over(w_running).alias("currency_max_tier_value"),
        F.count("tier_value_percent_rank").over(w_running).alias("currency_count_tier_value_percent_rank"),
        F.stddev_samp("tier_value_percent_rank").over(w_running).alias("currency_stddev_samp_tier_value_percent_rank"),
        F.rank().over(w5).alias("tier_value_percent_rank_rank"),
        F.percent_rank().over(w6).alias("tier_value_dense_rank_percent_rank"),
    )
)

totals = (
    scored_price_tiers
    .groupBy(
        "currency",
        F.when(F.col("tier_value_rank") >= 9, "PREMIUM").otherwise("STANDARD").alias("tier_value_rank_tier"),
    )
    .agg(
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.round(F.sum("currency_max_tier_value") / F.nullif(F.count(F.when(F.col("currency") == "EUR", 1)), F.lit(0)), 2).alias("currency_max_tier_value_per_eur"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.percentile_approx("currency_count_tier_value_percent_rank", 0.9).alias("currency_count_tier_value_percent_rank_p90"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
    )
    .filter(F.col("currency_count_tier_value_percent_rank_p90") < 0)
)

result = (
    totals
    .select(
        "currency",
        "tier_value_rank_tier",
        "num_currencies",
        "eur_count",
        "currency_count_tier_value_percent_rank_p90",
    )
    .orderBy("currency")
)
