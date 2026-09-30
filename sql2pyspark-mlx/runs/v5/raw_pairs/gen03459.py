from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_last())
w2 = Window.orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.orderBy(F.col("tier_value").desc())
w4 = Window.orderBy("tier_value")
w_grp = Window.orderBy(F.col("tier_value_cume_dist").desc_nulls_last())
w6 = Window.orderBy(F.col("tier_value_dense_rank_2").desc_nulls_first())
w7 = Window.orderBy(F.col("tier_value_dense_rank_plus_tier_value").asc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_limits").alias("tier_value"))
)

with_windows = (
    lines
    .select(
        "*",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank_2"),
    )
)

with_windows_price_tiers = (
    with_windows
    .select(
        "*",
        F.dense_rank().over(w).alias("tier_value_dense_rank_3"),
        F.cume_dist().over(w3).alias("tier_value_cume_dist"),
    )
)

metrics = (
    with_windows_price_tiers
    .withColumn("tier_value_rank", F.rank().over(w4))
    .withColumn("tier_value_cume_dist_cume_dist", F.cume_dist().over(w_grp))
)

valid = (
    metrics
    .filter(F.col("tier_value_dense_rank") != 6)
    .withColumn("tier_value_dense_rank_plus_tier_value", (F.col("tier_value_dense_rank") + F.col("tier_value")))
    .withColumn("effective_from_label", F.date_format("effective_from", "yyyy-MM-dd"))
)

base = (
    valid
    .filter(F.col("effective_from") >= F.lit("2024-03-04").cast("date"))
    .select(
        "*",
        F.col("tier_value_dense_rank_2").cast("double").alias("tier_value_dense_rank_2_num"),
        F.round((F.col("tier_value_rank") * 0.85), 2).alias("tier_value_rank_adjusted"),
    )
)

scored = (
    base
    .filter(F.col("effective_from") < "2024-12-12")
    .select(
        "*",
        F.cume_dist().over(w6).alias("tier_value_dense_rank_2_cume_dist"),
        F.percent_rank().over(w7).alias("tier_value_dense_rank_plus_tier_value_percent_rank"),
    )
)

result = (
    scored
    .groupBy(F.date_trunc("QUARTER", "effective_from").alias("quarter_start"))
    .agg(
        F.percentile_approx("tier_value_dense_rank_2", 0.9).alias("tier_value_dense_rank_2_p90"),
        F.percentile_approx("tier_value_rank_adjusted", [0.25, 0.5, 0.75]).alias("tier_value_rank_adjusted_quartiles"),
        F.count("*").alias("row_count"),
        F.round(F.avg("tier_value_dense_rank_2_cume_dist"), 2).alias("avg_tier_value_dense_rank_2_cume_dist"),
        F.percentile_approx("tier_value_dense_rank_3", 0.5).alias("tier_value_dense_rank_3_p50"),
        F.percentile_approx("tier_value_dense_rank_2_num", [0.25, 0.5, 0.75]).alias("tier_value_dense_rank_2_num_quartiles"),
    )
)
