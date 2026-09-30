from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency")
w2 = Window.orderBy("tier_value")
w_ordered = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value_percent_rank").desc_nulls_last())
w4 = Window.partitionBy("currency", "plan")
w5 = Window.orderBy(F.col("plan_stddev_samp_tier_value").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

enriched = (
    flattened
    .withColumn("plan_stddev_samp_tier_value", F.stddev_samp("tier_value").over(w))
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w2))
)

valid = (
    enriched
    .filter(F.col("plan").isin(["enterprise", "starter", "pro"]))
    .select(
        "id",
        "plan",
        "currency",
        "tier_value",
        "plan_stddev_samp_tier_value",
        "tier_value_percent_rank",
        F.upper("plan").alias("plan_upper"),
    )
)

metrics = (
    valid
    .withColumn("tier_value_percent_rank_cume_dist", F.cume_dist().over(w_ordered))
    .withColumn("currency_count_tier_value", F.count("tier_value").over(w4))
    .withColumn("plan_stddev_samp_tier_value_dense_rank", F.dense_rank().over(w5))
)

result = (
    metrics
    .select(
        "id",
        "plan",
        "currency",
        "tier_value",
        "plan_stddev_samp_tier_value",
        "tier_value_percent_rank",
        "plan_upper",
        "tier_value_percent_rank_cume_dist",
        "currency_count_tier_value",
    )
    .orderBy(F.col("tier_value_percent_rank_cume_dist").desc())
)
