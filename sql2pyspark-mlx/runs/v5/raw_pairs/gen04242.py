from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w_running = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value").asc_nulls_last())
w_running_2 = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value").desc())
w4 = Window.partitionBy("currency", "plan").orderBy(F.col("tier_value").desc_nulls_first())
w5 = Window.partitionBy("plan", "currency")

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("tier_value"))
)

cleaned = (
    flattened
    .filter(F.col("currency").isin(["THB", "EUR", "USD"]))
    .withColumn("currency_title", F.initcap(F.trim("currency")))
)

metrics = (
    cleaned
    .select(
        "id",
        "plan",
        "currency",
        "tier_value",
        "currency_title",
        F.avg("tier_value").over(w).alias("plan_avg_tier_value"),
        F.rank().over(w_running).alias("tier_value_rank"),
        F.cume_dist().over(w_running_2).alias("tier_value_cume_dist"),
        F.dense_rank().over(w4).alias("tier_value_dense_rank"),
        F.stddev_samp("tier_value").over(w5).alias("plan_stddev_samp_tier_value"),
    )
)

cleaned_price_tiers = (
    metrics
    .filter(F.col("plan_stddev_samp_tier_value") >= 43)
    .select(
        "id",
        "plan",
        "plan_avg_tier_value",
        "tier_value_rank",
        "tier_value_dense_rank",
        "plan_stddev_samp_tier_value",
        F.round((F.col("plan_avg_tier_value") * 1.07), 2).alias("plan_avg_tier_value_scaled"),
    )
)

filtered = (
    cleaned_price_tiers
    .filter(F.col("plan_avg_tier_value_scaled") <= 60)
    .select(
        "id",
        "plan",
        "tier_value_rank",
        "tier_value_dense_rank",
        "plan_stddev_samp_tier_value",
        "plan_avg_tier_value_scaled",
        (F.col("plan_avg_tier_value_scaled") / F.nullif("plan_stddev_samp_tier_value", F.lit(0))).alias("plan_avg_tier_value_scaled_per_plan_stddev_samp_tier_value"),
        (F.col("plan_avg_tier_value_scaled") - F.col("plan_stddev_samp_tier_value")).alias("plan_avg_tier_value_scaled_minus_plan_stddev_samp_tier_value"),
    )
)

with_windows = (
    filtered
    .filter(F.col("tier_value_rank") <= 5)
    .select(
        "id",
        "plan",
        "tier_value_rank",
        "plan_stddev_samp_tier_value",
        F.sum("tier_value_rank").over(w).alias("plan_sum_tier_value_rank"),
    )
)

result = (
    with_windows
    .groupBy("plan")
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("plan")).alias("num_plans_2"),
    )
)
