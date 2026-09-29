from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("tier_value").desc_nulls_last())
w3 = Window.partitionBy("plan").orderBy("tier_value")
w4 = Window.orderBy(F.col("plan_sum_tier_value").desc())
w5 = Window.partitionBy("plan").orderBy(F.col("tier_value_cume_dist").desc())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

with_windows = (
    exploded
    .select(
        "id",
        "plan",
        "effective_from",
        F.min("tier_value").over(w_running).alias("plan_min_tier_value"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank"),
        F.sum("tier_value").over(w_running).alias("plan_sum_tier_value"),
        F.cume_dist().over(w3).alias("tier_value_cume_dist"),
    )
)

kept_keys = (
    with_windows
    .select("plan", "id")
    .exceptAll(
        df_price_tiers
        .filter(F.col("plan") != "basic")
        .select("plan", "id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .withColumn("plan_sum_tier_value_rank", F.rank().over(w4))
    .withColumn("tier_value_cume_dist_cume_dist", F.cume_dist().over(w5))
    .withColumn("plan_sum_tier_value_cume_dist", F.sum("tier_value_cume_dist").over(w_running))
    .withColumn("plan_stddev_samp_tier_value_cume_dist", F.stddev_samp("tier_value_cume_dist").over(w_running))
)

valid = (
    metrics
    .filter(F.col("effective_from") >= F.lit("2024-02-02").cast("date"))
    .select(
        "id",
        "plan",
        "effective_from",
        "plan_min_tier_value",
        "tier_value_dense_rank",
        "plan_sum_tier_value",
        "tier_value_cume_dist",
        "tier_value_cume_dist_cume_dist",
        "plan_sum_tier_value_cume_dist",
        "plan_stddev_samp_tier_value_cume_dist",
        F.col("tier_value_dense_rank").cast("double").alias("tier_value_dense_rank_num"),
    )
)

result = (
    valid
    .groupBy("plan", F.date_trunc("WEEK", "effective_from").alias("week_start"))
    .agg(
        F.round(F.avg("plan_stddev_samp_tier_value_cume_dist"), 2).alias("avg_plan_stddev_samp_tier_value_cume_dist"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("tier_value_cume_dist"))).alias("enterprise_total_tier_value_cume_dist"),
        F.percentile_approx("plan_min_tier_value", 0.95).alias("plan_min_tier_value_p95"),
    )
)
