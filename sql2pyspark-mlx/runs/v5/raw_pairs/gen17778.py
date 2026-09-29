from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc())
w_ordered = Window.orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.orderBy(F.col("tier_value").desc_nulls_first())
w4 = Window.orderBy(F.col("tier_value_cume_dist").asc_nulls_last())
w5 = Window.orderBy(F.col("tier_value_cume_dist").desc_nulls_last())
w6 = Window.orderBy(F.col("tier_value_cume_dist_cume_dist").desc())
w7 = Window.orderBy(F.col("tier_value_cume_dist_cume_dist").asc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_limits").alias("tier_value"))
)

metrics = (
    lines
    .select(
        "effective_from",
        "tier_value",
        F.cume_dist().over(w).alias("tier_value_cume_dist"),
        F.cume_dist().over(w_ordered).alias("tier_value_cume_dist_2"),
        F.dense_rank().over(w3).alias("tier_value_dense_rank"),
    )
)

scored = (
    metrics
    .select(
        "effective_from",
        "tier_value_cume_dist_2",
        F.cume_dist().over(w4).alias("tier_value_cume_dist_cume_dist"),
        F.cume_dist().over(w5).alias("tier_value_cume_dist_cume_dist_2"),
    )
)

with_windows = (
    scored
    .filter(F.col("tier_value_cume_dist_cume_dist_2") < 0.63)
    .select(
        "effective_from",
        "tier_value_cume_dist_2",
        F.cume_dist().over(w6).alias("tier_value_cume_dist_cume_dist_cume_dist"),
        F.rank().over(w7).alias("tier_value_cume_dist_cume_dist_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("tier_value_cume_dist_cume_dist_rank") >= 7, "HIGH").otherwise("LOW").alias("tier_value_cume_dist_cume_dist_rank_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("tier_value_cume_dist_cume_dist_cume_dist", 0.9).alias("tier_value_cume_dist_cume_dist_cume_dist_p90"),
        F.sum("tier_value_cume_dist_cume_dist_cume_dist").alias("total_tier_value_cume_dist_cume_dist_cume_dist"),
    )
    .orderBy(F.col("tier_value_cume_dist_cume_dist_rank_tier").desc_nulls_first())
)
