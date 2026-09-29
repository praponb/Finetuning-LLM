from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.orderBy(F.col("tier_value").desc_nulls_last())
w4 = Window.orderBy(F.col("plan_sum_is_basic").desc())
w5 = Window.orderBy(F.col("plan_sum_is_basic").desc_nulls_last())
w6 = Window.orderBy(F.col("plan_sum_is_basic").asc_nulls_first())
w_part = Window.orderBy(F.col("plan_sum_is_basic_dense_rank").desc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_prices").alias("tier_value"),
    )
)

cleaned = (
    lines
    .filter((F.col("effective_from") >= F.lit("2024-03-03").cast("date")) & F.col("effective_from").between("2024-02-01", "2024-05-01"))
    .select("*", F.substring("plan", 1, 3).alias("plan_prefix"))
)

valid = (
    cleaned
    .filter((F.col("effective_from") >= F.lit("2024-04-14").cast("date")) & F.col("effective_from").between("2025-07-01", "2025-10-01"))
    .select(
        "plan",
        "effective_from",
        "tier_value",
        "plan_prefix",
        F.when(F.col("plan") == "basic", 1).otherwise(0).alias("is_basic"),
    )
)

scored = (
    valid
    .filter(F.col("tier_value").between(60, 61))
    .select(
        "effective_from",
        "tier_value",
        "plan_prefix",
        F.sum("is_basic").over(w_hist).alias("plan_sum_is_basic"),
    )
)

metrics = (
    scored
    .select(
        "effective_from",
        "tier_value",
        "plan_sum_is_basic",
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
        F.rank().over(w3).alias("tier_value_rank"),
        F.rank().over(w4).alias("plan_sum_is_basic_rank"),
        F.dense_rank().over(w5).alias("plan_sum_is_basic_dense_rank"),
    )
)

scored_price_tiers = metrics.withColumn("plan_sum_is_basic_cume_dist", F.cume_dist().over(w6))

metrics_price_tiers = (
    scored_price_tiers
    .select(
        "effective_from",
        "tier_value",
        "plan_sum_is_basic",
        "tier_value_cume_dist",
        "plan_sum_is_basic_rank",
        "plan_sum_is_basic_dense_rank",
        "plan_sum_is_basic_cume_dist",
        F.percent_rank().over(w_part).alias("plan_sum_is_basic_dense_rank_percent_rank"),
        F.percent_rank().over(w4).alias("plan_sum_is_basic_percent_rank"),
    )
)

result = (
    metrics_price_tiers
    .rollup(
        F.when(F.col("plan_sum_is_basic") >= 347, "HIGH").otherwise("LOW").alias("plan_sum_is_basic_tier"),
        F.date_trunc("QUARTER", "effective_from").alias("quarter_start"),
        F.when(F.col("plan_sum_is_basic_dense_rank_percent_rank") >= 0.8, "LARGE").otherwise("SMALL").alias("plan_sum_is_basic_dense_rank_percent_rank_tier"),
    )
    .agg(
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value"),
        F.min("plan_sum_is_basic_dense_rank_percent_rank").alias("min_plan_sum_is_basic_dense_rank_percent_rank"),
        F.percentile_approx("tier_value_cume_dist", [0.25, 0.5, 0.75]).alias("tier_value_cume_dist_quartiles"),
        F.avg("tier_value").alias("avg_tier_value_2"),
        F.percentile_approx("plan_sum_is_basic_dense_rank_percent_rank", [0.25, 0.5, 0.75]).alias("plan_sum_is_basic_dense_rank_percent_rank_quartiles"),
    )
    .filter(F.col("avg_tier_value") < 1000000)
)
