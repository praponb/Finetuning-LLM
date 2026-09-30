from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("tier_value").desc())
w_grp = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_last())
w4 = Window.orderBy(F.col("tier_value_dense_rank").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

scored = (
    exploded
    .withColumn("tier_value_rank", F.rank().over(w_recent))
    .withColumn("plan_count_tier_value", F.count("tier_value").over(w_grp))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w3))
)

kept_keys = (
    scored
    .select("id", "plan")
    .subtract(
        df_price_tiers
        .filter(F.col("currency") != "THB")
        .select("id", "plan")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "plan",
        "effective_from",
        "tier_value",
        "tier_value_rank",
        "tier_value_dense_rank",
        F.dense_rank().over(w4).alias("tier_value_dense_rank_dense_rank"),
        F.avg("tier_value_rank").over(w_grp).alias("plan_avg_tier_value_rank"),
    )
)

filtered = (
    with_windows
    .filter(F.col("tier_value_rank") == 8)
    .select(
        "plan",
        "tier_value",
        "tier_value_rank",
        "tier_value_dense_rank",
        "tier_value_dense_rank_dense_rank",
        F.when(F.col("plan_avg_tier_value_rank") >= 7, "High").when(F.col("plan_avg_tier_value_rank") >= 5, "Medium").otherwise("Low").alias("plan_avg_tier_value_rank_level"),
        F.round((F.col("tier_value_dense_rank_dense_rank") / F.col("plan_avg_tier_value_rank")), 2).alias("tier_value_dense_rank_dense_rank_per_plan_avg_tier_value_rank"),
    )
)

result = (
    filtered
    .withColumn("tier_value_dense_rank_dense_rank_per_plan_avg_tier_value_rank_tier", F.when(F.col("tier_value_dense_rank_dense_rank_per_plan_avg_tier_value_rank") >= 71, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["plan", "tier_value_dense_rank_dense_rank_per_plan_avg_tier_value_rank_tier"], ["plan"], []],
        "plan",
        "tier_value_dense_rank_dense_rank_per_plan_avg_tier_value_rank_tier",
    )
    .agg(
        F.percentile_approx("tier_value_dense_rank_dense_rank", 0.95).alias("tier_value_dense_rank_dense_rank_p95"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.sum(F.when(F.col("plan") == "starter", F.col("tier_value_dense_rank")).otherwise(0)).alias("starter_tier_value_dense_rank"),
        F.grouping("plan").alias("grouping_id"),
    )
    .orderBy(F.col("num_plans").asc_nulls_last())
)
