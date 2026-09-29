from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan")
w2 = Window.orderBy(F.col("tier_value").asc_nulls_last())
w3 = Window.partitionBy("plan")
w4 = Window.orderBy(F.col("tier_value_rank").desc())
w5 = Window.orderBy(F.col("effective_from_day").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

scored = (
    flattened
    .select(
        "*",
        F.count("tier_value").over(w).alias("currency_count_tier_value"),
        F.rank().over(w2).alias("tier_value_rank"),
        F.sum("tier_value").over(w3).alias("plan_sum_tier_value"),
    )
)

with_windows = (
    scored
    .select(
        "plan",
        "effective_from",
        "currency",
        "tier_value",
        "tier_value_rank",
        "plan_sum_tier_value",
        F.dense_rank().over(w4).alias("tier_value_rank_dense_rank"),
    )
)

cleaned = (
    with_windows
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-02-17"))) & F.col("currency").isin(["USD", "THB"]))
    .select(
        "plan",
        "effective_from",
        "plan_sum_tier_value",
        "tier_value_rank_dense_rank",
        F.ceil("tier_value_rank_dense_rank").alias("tier_value_rank_dense_rank_whole"),
        F.dayofmonth("effective_from").alias("effective_from_day"),
    )
)

metrics = (
    cleaned
    .withColumn("plan_stddev_samp_effective_from_day", F.stddev_samp("effective_from_day").over(w3))
    .withColumn("effective_from_day_cume_dist", F.cume_dist().over(w5))
)

cleaned_price_tiers = (
    metrics
    .filter(F.col("effective_from") < "2024-01-18")
    .withColumn("plan_group", F.when(F.col("plan").isin(["basic", "enterprise"]), "Tier 1").otherwise("Other"))
)

stats = (
    cleaned_price_tiers
    .groupBy(F.date_trunc("YEAR", "effective_from").alias("year_start"))
    .agg(
        F.round(F.avg("plan_sum_tier_value"), 2).alias("avg_plan_sum_tier_value"),
        F.sum(F.when(F.col("plan") == "starter", F.col("plan_stddev_samp_effective_from_day")).otherwise(0)).alias("starter_plan_stddev_samp_effective_from_day"),
        F.round(F.sum("tier_value_rank_dense_rank") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("tier_value_rank_dense_rank_per_basic"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
)

result = (
    stats
    .select(
        "year_start",
        "avg_plan_sum_tier_value",
        "starter_plan_stddev_samp_effective_from_day",
        "num_plans",
    )
)
