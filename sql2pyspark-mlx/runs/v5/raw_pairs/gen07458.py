from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_last())
w3 = Window.orderBy(F.col("tier_value_percent_rank").desc_nulls_last())
w4 = Window.orderBy(F.col("tier_value_percent_rank").desc_nulls_first())
w_ordered = Window.orderBy(F.col("tier_value").asc_nulls_first())
w_running = Window.orderBy(F.col("currency_sum_effective_from_day").asc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("tier_value"),
    )
)

valid = (
    exploded
    .filter(F.col("effective_from") >= F.lit("2024-05-19").cast("date"))
    .select("*", F.dayofmonth("effective_from").alias("effective_from_day"))
)

scored = (
    valid
    .filter(F.col("effective_from").between("2023-11-01", "2024-01-07"))
    .select(
        "id",
        "effective_from",
        "tier_value",
        "effective_from_day",
        F.count("tier_value").over(w).alias("currency_count_tier_value"),
        F.sum("effective_from_day").over(w).alias("currency_sum_effective_from_day"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
    )
)

metrics = (
    scored
    .withColumn("tier_value_percent_rank_dense_rank", F.dense_rank().over(w3))
    .withColumn("tier_value_percent_rank_percent_rank", F.percent_rank().over(w4))
    .withColumn("tier_value_rank", F.rank().over(w_ordered))
    .withColumn("currency_sum_effective_from_day_cume_dist", F.cume_dist().over(w_running))
)

valid_price_tiers = (
    metrics
    .filter(F.col("effective_from").between("2024-03-01", "2024-08-01"))
    .select(
        "id",
        "tier_value",
        "effective_from_day",
        "currency_count_tier_value",
        "tier_value_percent_rank_dense_rank",
        "tier_value_rank",
        F.coalesce("tier_value_percent_rank_dense_rank", F.lit(0)).alias("tier_value_percent_rank_dense_rank_filled"),
        F.coalesce("currency_sum_effective_from_day_cume_dist", F.lit(0)).alias("currency_sum_effective_from_day_cume_dist_filled"),
    )
)

cleaned = (
    valid_price_tiers
    .filter(F.col("tier_value_percent_rank_dense_rank_filled") > 22)
    .select("*", F.ceil("tier_value").alias("tier_value_whole"))
)

result = (
    cleaned
    .select(
        "id",
        "effective_from_day",
        "tier_value_rank",
        "currency_sum_effective_from_day_cume_dist_filled",
    )
    .orderBy(F.col("id").desc_nulls_last(), F.col("tier_value_rank").desc())
)
