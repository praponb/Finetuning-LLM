from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_first())
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.partitionBy("plan")
w4 = Window.orderBy(F.col("tier_value").desc_nulls_last())

lines = df_price_tiers.select("id", "plan", F.explode("tier_limits").alias("tier_value"))

enriched = (
    lines
    .select(
        "id",
        "plan",
        "tier_value",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank_2"),
        F.count("tier_value").over(w3).alias("plan_count_tier_value"),
        F.avg("tier_value").over(w3).alias("plan_avg_tier_value"),
        F.rank().over(w4).alias("tier_value_rank"),
    )
)

kept_keys = (
    enriched
    .select("plan")
    .intersectAll(
        df_price_tiers
        .filter(F.col("currency") == "THB")
        .select("plan")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "plan",
        "tier_value",
        "tier_value_dense_rank",
        "tier_value_dense_rank_2",
        "plan_count_tier_value",
        "plan_avg_tier_value",
        "tier_value_rank",
        F.max("tier_value_rank").over(w3).alias("plan_max_tier_value_rank"),
        F.min("tier_value_rank").over(w3).alias("plan_min_tier_value_rank"),
    )
)

valid = (
    scored
    .filter(F.col("tier_value") < 59)
    .withColumn("tier_value_per_plan_count_tier_value", F.round((F.col("tier_value") / F.col("plan_count_tier_value")), 2))
)

result = (
    valid
    .select(
        "plan",
        "tier_value",
        "tier_value_dense_rank",
        "tier_value_dense_rank_2",
        "plan_count_tier_value",
        "plan_avg_tier_value",
        "tier_value_rank",
        "tier_value_per_plan_count_tier_value",
    )
    .orderBy(F.col("tier_value_rank").desc_nulls_last())
)
