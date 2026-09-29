from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy("tier_value")
w3 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_last())
w4 = Window.orderBy(F.col("tier_value").desc())

flattened = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("tier_limits").alias("tier_value"))
)

filtered = (
    flattened
    .filter(F.col("tier_value") < 31)
    .withColumn("plan_prefix", F.substring("plan", 1, 2))
)

enriched = (
    filtered
    .filter(F.col("plan") == "enterprise")
    .select(
        "plan",
        "tier_value",
        "plan_prefix",
        F.count("tier_value").over(w).alias("plan_count_tier_value"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
        F.rank().over(w3).alias("tier_value_rank"),
        F.cume_dist().over(w4).alias("tier_value_cume_dist_2"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.round((F.col("plan_count_tier_value") / F.sum("plan_count_tier_value").over(w)), 4).alias("plan_count_tier_value_share"),
        F.stddev_samp("tier_value").over(w).alias("plan_stddev_samp_tier_value"),
    )
)

result = (
    with_windows
    .select(
        "tier_value",
        "plan_count_tier_value",
        "tier_value_cume_dist",
        "tier_value_rank",
        "plan_stddev_samp_tier_value",
    )
    .orderBy(F.col("plan_count_tier_value").asc_nulls_first())
)
