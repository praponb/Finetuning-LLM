from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("plan", "currency").orderBy("val")
w_running = Window.partitionBy("plan").orderBy(F.col("is_usd").desc())
w_recent = Window.partitionBy("plan")
w4 = Window.orderBy(F.col("val").asc_nulls_last())
w5 = Window.partitionBy("currency")
w6 = Window.orderBy(F.col("is_eur").asc_nulls_first())

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("val"))
)

cleaned = (
    exploded
    .filter(F.col("plan") == "basic")
    .withColumn("plan_currency", F.concat("plan", F.lit(" - "), "currency"))
    .withColumn("is_usd", F.when(F.col("currency") == "USD", 1).otherwise(0))
)

with_windows = (
    cleaned
    .select(
        "id",
        "currency",
        F.cume_dist().over(w_hist).alias("val_cume_dist"),
        F.percent_rank().over(w_running).alias("is_usd_percent_rank"),
        F.sum("val").over(w_recent).alias("plan_sum_val"),
        F.rank().over(w4).alias("val_rank"),
    )
)

filtered = (
    with_windows
    .filter(F.col("is_usd_percent_rank").isNull())
    .select(
        "id",
        "currency",
        "val_cume_dist",
        "is_usd_percent_rank",
        "plan_sum_val",
        F.trim("currency").alias("clean_currency"),
        F.when(F.col("currency") == "EUR", 1).otherwise(0).alias("is_eur"),
    )
)

cleaned_price_tiers = (
    filtered
    .filter(F.col("is_usd_percent_rank") < 0.52)
    .select(
        "*",
        F.coalesce("is_usd_percent_rank", F.lit(0)).alias("is_usd_percent_rank_filled"),
    )
)

cleaned_price_tiers_2 = (
    cleaned_price_tiers
    .filter(F.col("is_eur") > 21)
    .select(
        "id",
        "currency",
        "val_cume_dist",
        "is_usd_percent_rank",
        "plan_sum_val",
        "clean_currency",
        "is_eur",
        "is_usd_percent_rank_filled",
        F.ceil("is_usd_percent_rank_filled").alias("is_usd_percent_rank_filled_whole"),
    )
)

metrics = (
    cleaned_price_tiers_2
    .select(
        "*",
        F.stddev_samp("val_cume_dist").over(w5).alias("currency_stddev_samp_val_cume_dist"),
        F.rank().over(w6).alias("is_eur_rank"),
    )
)

result = (
    metrics
    .groupBy("currency")
    .agg(
        F.percentile_approx("val_cume_dist", 0.9).alias("val_cume_dist_p90"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
        F.round(F.avg("is_usd_percent_rank_filled_whole"), 2).alias("avg_is_usd_percent_rank_filled_whole"),
        F.percentile_approx("is_usd_percent_rank_filled_whole", [0.25, 0.5, 0.75]).alias("is_usd_percent_rank_filled_whole_quartiles"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
    )
)
