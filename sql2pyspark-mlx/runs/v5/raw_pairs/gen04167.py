from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("element").asc_nulls_first())
w3 = Window.orderBy(F.col("element_dense_rank").desc_nulls_first())
w4 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_first())
w5 = Window.partitionBy("plan").orderBy(F.col("days_since_effective_from").desc_nulls_last())
w_recent = Window.partitionBy("plan").orderBy(F.col("plan_stddev_samp_days_since_effective_from").desc_nulls_last())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_prices").alias("element"),
    )
)

valid = (
    flattened
    .filter(F.col("element").between(37, 58))
    .withColumn("days_since_effective_from", F.datediff(F.current_date(), "effective_from"))
)

scored = (
    valid
    .select(
        "*",
        F.stddev_samp("days_since_effective_from").over(w).alias("plan_stddev_samp_days_since_effective_from"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
        F.stddev_samp("element").over(w).alias("plan_stddev_samp_element"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.percent_rank().over(w3).alias("element_dense_rank_percent_rank"),
        F.dense_rank().over(w4).alias("element_dense_rank_2"),
    )
)

metrics = (
    with_windows
    .filter(F.col("element_dense_rank_2") >= 5)
    .select(
        "id",
        "plan",
        F.max("plan_stddev_samp_days_since_effective_from").over(w).alias("plan_max_plan_stddev_samp_days_since_effective_from"),
        F.cume_dist().over(w5).alias("days_since_effective_from_cume_dist"),
        F.dense_rank().over(w_recent).alias("plan_stddev_samp_days_since_effective_from_dense_rank"),
        F.min("element_dense_rank_percent_rank").over(w).alias("plan_min_element_dense_rank_percent_rank"),
    )
)

valid_price_tiers = (
    metrics
    .filter(F.col("plan_max_plan_stddev_samp_days_since_effective_from").isNotNull())
    .select(
        "plan",
        "plan_max_plan_stddev_samp_days_since_effective_from",
        "days_since_effective_from_cume_dist",
        "plan_stddev_samp_days_since_effective_from_dense_rank",
        (F.col("days_since_effective_from_cume_dist") * F.col("plan_stddev_samp_days_since_effective_from_dense_rank")).alias("days_since_effective_from_cume_dist_times_plan_stddev_samp_days_since_effective_from_dense_rank"),
    )
)

result = (
    valid_price_tiers
    .groupBy("plan")
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.percentile_approx("plan_max_plan_stddev_samp_days_since_effective_from", 0.95).alias("plan_max_plan_stddev_samp_days_since_effective_from_p95"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.countDistinct("plan").alias("distinct_plans"),
    )
    .orderBy(F.col("enterprise_count").asc_nulls_last())
)
