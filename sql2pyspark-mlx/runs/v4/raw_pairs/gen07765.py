from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("tier_value").asc_nulls_first())
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.orderBy(F.col("tier_value").desc_nulls_first())
w_grp = Window.partitionBy("plan")
w5 = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_first())
w6 = Window.partitionBy("plan").orderBy(F.col("tier_value_cume_dist_3").desc_nulls_first())
w7 = Window.orderBy("plan_stddev_samp_tier_value")
w8 = Window.orderBy(F.col("plan_stddev_samp_tier_value").desc())
w9 = Window.orderBy(F.col("tier_value_cume_dist_4").asc_nulls_first())
w10 = Window.orderBy(F.col("tier_value_rank").asc_nulls_last())
w11 = Window.partitionBy("plan").orderBy(F.col("plan_max_plan_stddev_samp_tier_value").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_prices").alias("tier_value"),
    )
)

metrics = (
    exploded
    .withColumn("tier_value_rank", F.rank().over(w_running))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w2))
    .withColumn("tier_value_cume_dist_2", F.cume_dist().over(w3))
    .withColumn("plan_stddev_samp_tier_value", F.stddev_samp("tier_value").over(w_grp))
    .withColumn("tier_value_cume_dist_3", F.cume_dist().over(w5))
)

enriched = (
    metrics
    .select(
        "*",
        F.round((F.col("tier_value_rank") / F.sum("tier_value_rank").over(w_grp)), 4).alias("tier_value_rank_share"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist_4"),
    )
)

scored = (
    enriched
    .withColumn("plan_max_plan_stddev_samp_tier_value", F.max("plan_stddev_samp_tier_value").over(w_grp))
    .withColumn("tier_value_cume_dist_3_rank", F.rank().over(w6))
    .withColumn("plan_stddev_samp_tier_value_dense_rank", F.dense_rank().over(w7))
    .withColumn("plan_stddev_samp_tier_value_percent_rank", F.percent_rank().over(w8))
)

scored_price_tiers = (
    scored
    .select(
        "*",
        F.dense_rank().over(w9).alias("tier_value_cume_dist_4_dense_rank"),
        F.percent_rank().over(w10).alias("tier_value_rank_percent_rank"),
        F.rank().over(w11).alias("plan_max_plan_stddev_samp_tier_value_rank"),
    )
)

cleaned = (
    scored_price_tiers
    .filter((F.col("effective_from") > F.lit("2024-04-27").cast("date")) & (F.col("plan_stddev_samp_tier_value_percent_rank") < 0.23))
    .select(
        "id",
        "plan",
        "tier_value_rank",
        "plan_stddev_samp_tier_value",
        "tier_value_cume_dist_3",
        "tier_value_rank_share",
        "plan_max_plan_stddev_samp_tier_value",
        "plan_stddev_samp_tier_value_dense_rank",
        "plan_stddev_samp_tier_value_percent_rank",
        "tier_value_rank_percent_rank",
        F.coalesce("tier_value_cume_dist_2", F.lit(0)).alias("tier_value_cume_dist_2_filled"),
        (F.col("tier_value_rank_percent_rank") / F.nullif("tier_value_cume_dist_4", F.lit(0))).alias("tier_value_rank_percent_rank_per_tier_value_cume_dist_4"),
    )
)

report = (
    cleaned
    .select(
        "id",
        "plan",
        "tier_value_rank",
        "tier_value_rank_share",
        "plan_max_plan_stddev_samp_tier_value",
        "plan_stddev_samp_tier_value_dense_rank",
        "tier_value_rank_percent_rank",
        "tier_value_cume_dist_2_filled",
        "tier_value_rank_percent_rank_per_tier_value_cume_dist_4",
        F.when(F.col("plan").rlike("^(starter|enterprise)$"), "PRIORITY").otherwise("EXTENDED").alias("plan_zone"),
    )
    .orderBy(F.col("tier_value_rank").asc_nulls_first())
)

result = (
    report
    .select(
        "id",
        "plan",
        "plan_max_plan_stddev_samp_tier_value",
        "plan_stddev_samp_tier_value_dense_rank",
        "tier_value_rank_percent_rank",
        "tier_value_rank_percent_rank_per_tier_value_cume_dist_4",
    )
    .orderBy(
        F.col("tier_value_rank_percent_rank").desc(),
        "plan_max_plan_stddev_samp_tier_value",
    )
)
