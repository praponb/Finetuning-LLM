from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("tier_value")
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("currency", "plan")
w5 = Window.orderBy(F.col("tier_value_dense_rank").desc_nulls_last())
w6 = Window.orderBy(F.col("currency_sum_tier_value_plus_plan_min_tier_value").desc_nulls_last())
w7 = Window.orderBy(F.col("plan_stddev_samp_plan_count_currency_sum_tier_value").desc_nulls_last())
w8 = Window.orderBy(F.col("plan_stddev_samp_plan_count_currency_sum_tier_value").asc_nulls_last())
w9 = Window.orderBy(F.col("plan_count_currency_sum_tier_value_times_currency_sum_tier_value").desc())
w_recent = Window.orderBy(F.col("plan_stddev_samp_plan_count_currency_sum_tier_value_dense_rank").desc_nulls_first())
w_running = Window.orderBy(F.col("plan_stddev_samp_plan_count_currency_sum_tier_value_cume_dist").desc())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

enriched = (
    exploded
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
        F.stddev_samp("tier_value").over(w2).alias("currency_stddev_samp_tier_value"),
        F.min("tier_value").over(w3).alias("plan_min_tier_value"),
        F.sum("tier_value").over(w4).alias("currency_sum_tier_value"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.count("currency_sum_tier_value").over(w3).alias("plan_count_currency_sum_tier_value"),
        F.stddev_samp("currency_stddev_samp_tier_value").over(w3).alias("plan_stddev_samp_currency_stddev_samp_tier_value"),
        F.percent_rank().over(w5).alias("tier_value_dense_rank_percent_rank"),
    )
)

valid = (
    scored
    .filter(F.col("currency_sum_tier_value").isNotNull())
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_value_dense_rank",
        "currency_sum_tier_value",
        "plan_count_currency_sum_tier_value",
        "plan_stddev_samp_currency_stddev_samp_tier_value",
        F.coalesce("plan_min_tier_value", F.lit(0)).alias("plan_min_tier_value_filled"),
        (F.col("currency_sum_tier_value") + F.col("plan_min_tier_value")).alias("currency_sum_tier_value_plus_plan_min_tier_value"),
    )
)

filtered = (
    valid
    .filter((F.col("effective_from") > F.lit("2024-04-10").cast("date")) & (F.col("plan") == "enterprise"))
    .withColumn("currency_sum_tier_value_plus_plan_min_tier_value_plus_plan_min_tier_value_filled", (F.col("currency_sum_tier_value_plus_plan_min_tier_value") + F.col("plan_min_tier_value_filled")))
    .withColumn("plan_count_currency_sum_tier_value_times_currency_sum_tier_value", (F.col("plan_count_currency_sum_tier_value") * F.col("currency_sum_tier_value")))
)

enriched_price_tiers = (
    filtered
    .select(
        "id",
        "plan_count_currency_sum_tier_value",
        "plan_stddev_samp_currency_stddev_samp_tier_value",
        "plan_min_tier_value_filled",
        "currency_sum_tier_value_plus_plan_min_tier_value",
        "plan_count_currency_sum_tier_value_times_currency_sum_tier_value",
        F.stddev_samp("plan_count_currency_sum_tier_value").over(w3).alias("plan_stddev_samp_plan_count_currency_sum_tier_value"),
        F.stddev_samp("currency_sum_tier_value").over(w4).alias("currency_stddev_samp_currency_sum_tier_value"),
    )
)

with_windows = (
    enriched_price_tiers
    .select(
        "id",
        "plan_count_currency_sum_tier_value",
        "plan_count_currency_sum_tier_value_times_currency_sum_tier_value",
        "plan_stddev_samp_plan_count_currency_sum_tier_value",
        "currency_stddev_samp_currency_sum_tier_value",
        F.percent_rank().over(w6).alias("currency_sum_tier_value_plus_plan_min_tier_value_percent_rank"),
        F.cume_dist().over(w7).alias("plan_stddev_samp_plan_count_currency_sum_tier_value_cume_dist"),
        F.dense_rank().over(w8).alias("plan_stddev_samp_plan_count_currency_sum_tier_value_dense_rank"),
        F.dense_rank().over(w9).alias("plan_count_currency_sum_tier_value_times_currency_sum_tier_value_dense_rank"),
    )
)

result = (
    with_windows
    .filter(F.col("currency_sum_tier_value_plus_plan_min_tier_value_percent_rank").between(0.35, 0.73))
    .withColumn("plan_stddev_samp_plan_count_currency_sum_tier_value_dense_rank_cume_dist", F.cume_dist().over(w_recent))
    .withColumn("plan_stddev_samp_plan_count_currency_sum_tier_value_cume_dist_dense_rank", F.dense_rank().over(w_running))
    .withColumn("plan_stddev_samp_plan_count_currency_sum_tier_value_dense_rank_dense_rank", F.dense_rank().over(w_recent))
)
