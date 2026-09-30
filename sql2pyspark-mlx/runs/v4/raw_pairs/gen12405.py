from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_last())
w_hist = Window.orderBy(F.col("tier_value").desc_nulls_last())
w_running = Window.orderBy(F.col("tier_value").desc())
w4 = Window.orderBy(F.col("tier_value_cume_dist").desc_nulls_last())
w5 = Window.orderBy(F.col("tier_value_cume_dist").asc_nulls_first())
w_recent = Window.orderBy(F.col("tier_value_percent_rank_3").asc_nulls_last())
w_recent_2 = Window.orderBy(F.col("tier_value_percent_rank_3").desc_nulls_last())
w_grp = Window.orderBy(F.col("tier_value_percent_rank_3_dense_rank").desc())
w9 = Window.orderBy(F.col("tier_value_percent_rank_3_dense_rank").desc_nulls_last())
w10 = Window.orderBy(F.col("tier_value_rank").asc_nulls_last())
w11 = Window.orderBy(F.col("tier_value_cume_dist_rank").asc_nulls_last())

flattened = df_price_tiers.select("id", F.explode("tier_prices").alias("tier_value"))

enriched = (
    flattened
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w))
    .withColumn("tier_value_rank", F.rank().over(w_hist))
)

scored = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w_running).alias("tier_value_percent_rank"),
        F.cume_dist().over(w4).alias("tier_value_cume_dist_cume_dist"),
        F.percent_rank().over(w_running).alias("tier_value_percent_rank_2"),
        F.percent_rank().over(w).alias("tier_value_percent_rank_3"),
        F.rank().over(w5).alias("tier_value_cume_dist_rank"),
    )
)

metrics = (
    scored
    .withColumn("tier_value_percent_rank_3_dense_rank", F.dense_rank().over(w_recent))
    .withColumn("tier_value_percent_rank_3_percent_rank", F.percent_rank().over(w_recent_2))
)

scored_price_tiers = (
    metrics
    .select(
        "*",
        F.rank().over(w_grp).alias("tier_value_percent_rank_3_dense_rank_rank"),
        F.percent_rank().over(w9).alias("tier_value_percent_rank_3_dense_rank_percent_rank"),
    )
)

with_windows = (
    scored_price_tiers
    .withColumn("tier_value_rank_dense_rank", F.dense_rank().over(w10))
    .withColumn("tier_value_percent_rank_3_dense_rank_2", F.dense_rank().over(w_recent_2))
    .withColumn("tier_value_cume_dist_rank_percent_rank", F.percent_rank().over(w11))
)

base = (
    with_windows
    .filter(F.col("tier_value_percent_rank_3_dense_rank_2") == 8)
    .select(
        "id",
        "tier_value",
        "tier_value_cume_dist",
        "tier_value_percent_rank",
        "tier_value_cume_dist_cume_dist",
        "tier_value_percent_rank_2",
        "tier_value_percent_rank_3",
        "tier_value_cume_dist_rank",
        "tier_value_percent_rank_3_dense_rank",
        "tier_value_percent_rank_3_percent_rank",
        "tier_value_percent_rank_3_dense_rank_percent_rank",
        "tier_value_rank_dense_rank",
        "tier_value_percent_rank_3_dense_rank_2",
        (F.col("tier_value_percent_rank_2") * F.col("tier_value_percent_rank_3_dense_rank_percent_rank")).alias("tier_value_percent_rank_2_times_tier_value_percent_rank_3_dense_rank_percent_rank"),
    )
)

result = (
    base
    .select(
        "id",
        "tier_value",
        "tier_value_cume_dist",
        "tier_value_cume_dist_cume_dist",
        "tier_value_percent_rank_3_dense_rank",
        "tier_value_percent_rank_3_percent_rank",
        "tier_value_percent_rank_3_dense_rank_percent_rank",
        "tier_value_rank_dense_rank",
        "tier_value_percent_rank_3_dense_rank_2",
    )
    .orderBy(F.col("tier_value_percent_rank_3_dense_rank").desc_nulls_first())
)
