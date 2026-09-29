from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("tier_value").desc_nulls_first())
w_running = Window.orderBy("tier_value")
w3 = Window.orderBy(F.col("tier_value").desc_nulls_last())
w4 = Window.orderBy(F.col("tier_value").asc_nulls_last())
w5 = Window.orderBy("tier_value_dense_rank_2")
w6 = Window.orderBy(F.col("tier_value_percent_rank").desc_nulls_last())
w7 = Window.orderBy(F.col("tier_value_dense_rank").asc_nulls_first())
w8 = Window.orderBy(F.col("tier_value_dense_rank_percent_rank").asc_nulls_first())

flattened = df_price_tiers.select("id", F.explode("tier_limits").alias("tier_value"))

enriched = (
    flattened
    .select(
        "*",
        F.dense_rank().over(w_recent).alias("tier_value_dense_rank"),
        F.rank().over(w_running).alias("tier_value_rank"),
        F.percent_rank().over(w3).alias("tier_value_percent_rank"),
        F.rank().over(w4).alias("tier_value_rank_2"),
        F.dense_rank().over(w_recent).alias("tier_value_dense_rank_2"),
    )
)

enriched_price_tiers = (
    enriched
    .select(
        "*",
        F.rank().over(w5).alias("tier_value_dense_rank_2_rank"),
        F.rank().over(w6).alias("tier_value_percent_rank_rank"),
        F.percent_rank().over(w7).alias("tier_value_dense_rank_percent_rank"),
    )
)

scored = (
    enriched_price_tiers
    .select(
        "*",
        F.rank().over(w_running).alias("tier_value_rank_3"),
        F.percent_rank().over(w8).alias("tier_value_dense_rank_percent_rank_percent_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("tier_value_dense_rank_percent_rank_percent_rank") >= 0.69, "HIGH").otherwise("LOW").alias("tier_value_dense_rank_percent_rank_percent_rank_tier"),
    )
    .agg(
        F.percentile_approx("tier_value_rank_2", 0.5).alias("tier_value_rank_2_p50"),
        F.percentile_approx("tier_value_dense_rank_2_rank", [0.25, 0.5, 0.75]).alias("tier_value_dense_rank_2_rank_quartiles"),
        F.round(F.avg("tier_value_dense_rank_2_rank"), 2).alias("avg_tier_value_dense_rank_2_rank"),
        F.percentile_approx("tier_value_dense_rank_2_rank", [0.25, 0.5, 0.75]).alias("tier_value_dense_rank_2_rank_quartiles_2"),
        F.avg("tier_value_dense_rank_percent_rank").alias("avg_tier_value_dense_rank_percent_rank"),
        F.percentile_approx("tier_value_rank", 0.95).alias("tier_value_rank_p95"),
    )
)
