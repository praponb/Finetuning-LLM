from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_last())
w3 = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_last())
w4 = Window.partitionBy("plan").orderBy(F.col("plan_sum_tier_value").asc_nulls_first())
w_grp = Window.partitionBy("plan").orderBy(F.col("tier_value_rank_share").desc())
w6 = Window.orderBy(F.col("plan_sum_tier_value").desc())
w7 = Window.partitionBy("plan").orderBy(F.col("tier_value_rank_share").desc_nulls_first())
w8 = Window.partitionBy("plan").orderBy(F.col("tier_value_cume_dist").desc())
w9 = Window.partitionBy("year_start").orderBy(F.col("enterprise_tier_value_cume_dist").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_limits").alias("tier_value"),
    )
)

metrics = (
    flattened
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_value",
        F.min("tier_value").over(w).alias("plan_min_tier_value"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
        F.sum("tier_value").over(w).alias("plan_sum_tier_value"),
        F.rank().over(w3).alias("tier_value_rank"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.round((F.col("tier_value_rank") / F.sum("tier_value_rank").over(w)), 4).alias("tier_value_rank_share"),
        F.cume_dist().over(w4).alias("plan_sum_tier_value_cume_dist"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.cume_dist().over(w2).alias("tier_value_cume_dist_2"),
        F.dense_rank().over(w_grp).alias("tier_value_rank_share_dense_rank"),
        F.cume_dist().over(w6).alias("plan_sum_tier_value_cume_dist_2"),
        F.dense_rank().over(w7).alias("tier_value_rank_share_dense_rank_2"),
    )
)

scored_price_tiers = (
    scored
    .filter(F.col("tier_value_cume_dist_2") >= 0.73)
    .select(
        "*",
        F.percent_rank().over(w8).alias("tier_value_cume_dist_percent_rank"),
        F.count("tier_value_cume_dist").over(w).alias("plan_count_tier_value_cume_dist"),
    )
)

summary = (
    scored_price_tiers
    .groupBy(
        F.date_trunc("YEAR", "effective_from").alias("year_start"),
        F.when(F.col("tier_value_cume_dist_2") >= 0.76, "LARGE").otherwise("SMALL").alias("tier_value_cume_dist_2_tier"),
    )
    .agg(
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("tier_value_cume_dist")).otherwise(0)).alias("enterprise_tier_value_cume_dist"),
        F.count_if(F.col("plan") == "starter").alias("starter_events"),
    )
    .withColumn("enterprise_tier_value_cume_dist_percent_rank", F.percent_rank().over(w9))
)

result = (
    summary
    .select(
        "year_start",
        "tier_value_cume_dist_2_tier",
        "basic_events",
        "enterprise_tier_value_cume_dist",
        "starter_events",
        "enterprise_tier_value_cume_dist_percent_rank",
    )
    .orderBy(F.col("tier_value_cume_dist_2_tier").asc_nulls_last())
)
