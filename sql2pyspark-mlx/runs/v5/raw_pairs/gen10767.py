from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.partitionBy("plan").orderBy(F.col("plan_stddev_samp_tier_value_2").desc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_limits").alias("tier_value"),
    )
)

enriched = (
    lines
    .select(
        "*",
        F.stddev_samp("tier_value").over(w_ordered).alias("plan_stddev_samp_tier_value"),
        F.max("tier_value").over(w_ordered).alias("plan_max_tier_value"),
        F.stddev_samp("tier_value").over(w_ordered).alias("plan_stddev_samp_tier_value_2"),
    )
)

with_windows = (
    enriched
    .filter(F.col("plan") == "pro")
    .select(
        "tier_value",
        "plan_stddev_samp_tier_value",
        "plan_max_tier_value",
        "plan_stddev_samp_tier_value_2",
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
        F.rank().over(w3).alias("plan_stddev_samp_tier_value_2_rank"),
    )
)

filtered = (
    with_windows
    .filter(F.col("plan_stddev_samp_tier_value").isNotNull())
    .select(
        "tier_value",
        "plan_max_tier_value",
        "plan_stddev_samp_tier_value_2",
        F.round("tier_value", 1).alias("tier_value_rounded"),
    )
)

valid = (
    filtered
    .filter(F.col("plan_max_tier_value") > 72)
    .withColumn("greatest_tier_value_rounded_plan_stddev_samp_tier_value_2", F.greatest("tier_value_rounded", "plan_stddev_samp_tier_value_2"))
)

result = (
    valid
    .select(
        "tier_value",
        "plan_max_tier_value",
        "plan_stddev_samp_tier_value_2",
        "tier_value_rounded",
    )
    .orderBy(F.col("plan_stddev_samp_tier_value_2").desc_nulls_last())
)
