from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_last())
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())
w3 = Window.partitionBy("plan").orderBy(F.col("tier_value_cume_dist").asc_nulls_last())
w4 = Window.partitionBy("plan").orderBy(F.col("tier_value_cume_dist_2").asc_nulls_first())
w5 = Window.partitionBy("plan").orderBy(F.col("tier_value_cume_dist_2").desc())
w_running = Window.partitionBy("plan").orderBy("tier_value_cume_dist_2_rank")
w7 = Window.orderBy(F.col("tier_value_cume_dist_2_rank").desc())
w8 = Window.partitionBy("plan")
w9 = Window.orderBy("tier_value_cume_dist_2_rank_percent_rank")
w_grp = Window.orderBy(F.col("tier_value_cume_dist_2").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

scored = (
    exploded
    .select(
        "*",
        F.cume_dist().over(w).alias("tier_value_cume_dist"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist_2"),
    )
)

enriched = (
    scored
    .withColumn("tier_value_cume_dist_cume_dist", F.cume_dist().over(w3))
    .withColumn("tier_value_cume_dist_2_rank", F.rank().over(w4))
)

enriched_price_tiers = (
    enriched
    .withColumn("tier_value_cume_dist_2_cume_dist", F.cume_dist().over(w5))
    .withColumn("tier_value_cume_dist_2_rank_dense_rank", F.dense_rank().over(w_running))
    .withColumn("tier_value_cume_dist_2_rank_percent_rank", F.percent_rank().over(w7))
)

with_windows = (
    enriched_price_tiers
    .select(
        "*",
        F.avg("tier_value").over(w8).alias("plan_avg_tier_value"),
        F.dense_rank().over(w5).alias("tier_value_cume_dist_2_dense_rank"),
        F.rank().over(w9).alias("tier_value_cume_dist_2_rank_percent_rank_rank"),
        F.rank().over(w_grp).alias("tier_value_cume_dist_2_rank_2"),
        F.sum("tier_value_cume_dist_2_cume_dist").over(w8).alias("plan_sum_tier_value_cume_dist_2_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "plan",
        "tier_value_cume_dist",
        "tier_value_cume_dist_2",
        "tier_value_cume_dist_cume_dist",
        "tier_value_cume_dist_2_cume_dist",
        "tier_value_cume_dist_2_rank_dense_rank",
        "tier_value_cume_dist_2_rank_percent_rank",
        "tier_value_cume_dist_2_dense_rank",
        F.when(F.col("plan").rlike("^(starter|enterprise)$"), "PRIORITY").when(F.col("plan").rlike("^(basic|pro)$"), "GROUP_A").otherwise("EXTENDED").alias("plan_zone"),
    )
)
