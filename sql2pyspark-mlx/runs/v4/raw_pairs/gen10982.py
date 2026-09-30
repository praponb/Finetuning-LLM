from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("currency")
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_last())
w3 = Window.partitionBy("plan")
w_hist = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value").desc())
w5 = Window.partitionBy("plan").orderBy(F.col("tier_value_cume_dist").desc_nulls_first())
w6 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())
w7 = Window.partitionBy("plan").orderBy(F.col("tier_value_cume_dist_cume_dist").desc_nulls_last())
w8 = Window.orderBy(F.col("plan_sum_tier_value").desc())
w9 = Window.partitionBy("plan", "currency")
w10 = Window.partitionBy("currency").orderBy(F.col("tier_value_cume_dist_cume_dist").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

remaining_keys = (
    lines
    .select("plan", "currency")
    .intersect(
        df_price_tiers
        .filter(F.col("effective_from").between("2023-07-01", "2025-01-12"))
        .select("plan", "currency")
    )
)

verified = (
    lines
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .withColumn("currency_max_tier_value", F.max("tier_value").over(w_recent))
    .withColumn("tier_value_rank", F.rank().over(w2))
    .withColumn("currency_avg_tier_value", F.avg("tier_value").over(w_recent))
    .withColumn("plan_sum_tier_value", F.sum("tier_value").over(w3))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w_hist))
)

with_windows = (
    enriched
    .select(
        "*",
        F.cume_dist().over(w5).alias("tier_value_cume_dist_cume_dist"),
        F.sum("plan_sum_tier_value").over(w_recent).alias("currency_sum_plan_sum_tier_value"),
        F.dense_rank().over(w6).alias("tier_value_dense_rank"),
    )
)

enriched_price_tiers = (
    with_windows
    .withColumn("tier_value_rank_share", F.round((F.col("tier_value_rank") / F.sum("tier_value_rank").over(w_recent)), 4))
    .withColumn("tier_value_cume_dist_cume_dist_dense_rank", F.dense_rank().over(w7))
)

metrics = (
    enriched_price_tiers
    .withColumn("plan_min_currency_sum_plan_sum_tier_value", F.min("currency_sum_plan_sum_tier_value").over(w3))
    .withColumn("tier_value_dense_rank_share", F.round((F.col("tier_value_dense_rank") / F.sum("tier_value_dense_rank").over(w_recent)), 4))
    .withColumn("plan_sum_tier_value_percent_rank", F.percent_rank().over(w8))
    .withColumn("plan_stddev_samp_plan_sum_tier_value", F.stddev_samp("plan_sum_tier_value").over(w9))
    .withColumn("tier_value_cume_dist_cume_dist_dense_rank_2", F.dense_rank().over(w10))
)

base = (
    metrics
    .filter(F.col("tier_value_dense_rank_share").between(0.68, 0.72))
    .select("*", F.floor("tier_value").alias("tier_value_whole"))
)

result = (
    base
    .select(
        "plan",
        "tier_value",
        "tier_value_rank",
        "plan_sum_tier_value",
        "tier_value_cume_dist",
        "tier_value_cume_dist_cume_dist",
        "currency_sum_plan_sum_tier_value",
        "tier_value_dense_rank",
        "tier_value_rank_share",
        F.when(F.col("plan").rlike("^(pro|starter)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("plan_zone"),
    )
    .orderBy("tier_value_cume_dist_cume_dist", F.col("tier_value_rank_share").desc())
)
