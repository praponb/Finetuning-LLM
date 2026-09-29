from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())
w3 = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_first())

exploded = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("tier_prices").alias("tier_value"))
)

with_windows = (
    exploded
    .withColumn("plan_sum_tier_value", F.sum("tier_value").over(w_grp))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w2))
    .withColumn("plan_stddev_samp_tier_value", F.stddev_samp("tier_value").over(w_grp))
)

base = (
    with_windows
    .filter(F.col("tier_value").between(48, 76))
    .select(
        "*",
        F.round("plan_stddev_samp_tier_value", 1).alias("plan_stddev_samp_tier_value_rounded"),
    )
)

with_windows_price_tiers = (
    base
    .withColumn("plan_stddev_samp_tier_value_2", F.stddev_samp("tier_value").over(w_grp))
    .withColumn("plan_avg_plan_stddev_samp_tier_value_rounded", F.avg("plan_stddev_samp_tier_value_rounded").over(w_grp))
    .withColumn("tier_value_rank", F.rank().over(w3))
    .withColumn("plan_sum_plan_stddev_samp_tier_value", F.sum("plan_stddev_samp_tier_value").over(w_grp))
)

valid = (
    with_windows_price_tiers
    .filter(F.col("plan").isin(["starter", "enterprise"]))
    .select(
        "*",
        F.coalesce("tier_value", F.lit(0)).alias("tier_value_filled"),
        F.col("plan_sum_plan_stddev_samp_tier_value").cast("double").alias("plan_sum_plan_stddev_samp_tier_value_num"),
    )
)

with_windows_price_tiers_2 = (
    valid
    .select(
        "*",
        F.avg("plan_stddev_samp_tier_value_rounded").over(w_grp).alias("plan_avg_plan_stddev_samp_tier_value_rounded_2"),
        F.count("tier_value_rank").over(w_grp).alias("plan_count_tier_value_rank"),
    )
)

valid_price_tiers = (
    with_windows_price_tiers_2
    .filter(F.col("plan").isin(["pro", "enterprise"]))
    .select(
        "plan",
        "tier_value",
        "tier_value_cume_dist",
        "plan_stddev_samp_tier_value",
        "plan_stddev_samp_tier_value_rounded",
        "plan_avg_plan_stddev_samp_tier_value_rounded",
        "plan_sum_plan_stddev_samp_tier_value",
        "tier_value_filled",
        "plan_sum_plan_stddev_samp_tier_value_num",
        "plan_avg_plan_stddev_samp_tier_value_rounded_2",
        "plan_count_tier_value_rank",
        (F.col("tier_value_cume_dist") / F.nullif("plan_stddev_samp_tier_value", F.lit(0))).alias("tier_value_cume_dist_per_plan_stddev_samp_tier_value"),
    )
)

result = (
    valid_price_tiers
    .select(
        "plan",
        "tier_value",
        "tier_value_cume_dist",
        "plan_stddev_samp_tier_value",
        "plan_stddev_samp_tier_value_rounded",
        "plan_sum_plan_stddev_samp_tier_value",
        "tier_value_filled",
        "plan_sum_plan_stddev_samp_tier_value_num",
        "plan_avg_plan_stddev_samp_tier_value_rounded_2",
        F.when(F.col("plan").rlike("^(enterprise|starter)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("plan_zone"),
    )
    .orderBy(F.col("plan_stddev_samp_tier_value").desc_nulls_last())
)
