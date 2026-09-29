from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_last())
w2 = Window.orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.orderBy(F.col("tier_value").asc_nulls_first())
w4 = Window.orderBy(F.col("adjusted_tier_value").desc_nulls_last())
w5 = Window.orderBy("tier_value_dense_rank")

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_prices").alias("tier_value"))
)

with_windows = (
    lines
    .select(
        "id",
        "effective_from",
        "tier_value",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.cume_dist().over(w).alias("tier_value_cume_dist"),
        F.rank().over(w2).alias("tier_value_rank"),
        F.dense_rank().over(w3).alias("tier_value_dense_rank"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-03-19")))
    .select(
        "*",
        F.add_months("effective_from", 12).alias("effective_from_plus_months"),
        F.round((F.col("tier_value") * 1.15), 2).alias("adjusted_tier_value"),
    )
)

valid = (
    cleaned
    .filter(F.col("effective_from") > F.lit("2024-02-18").cast("date"))
    .select(
        "id",
        "effective_from",
        "tier_value_rank",
        "tier_value_dense_rank",
        "adjusted_tier_value",
        F.round("tier_value_percent_rank", 2).alias("tier_value_percent_rank_rounded"),
    )
)

with_windows_price_tiers = (
    valid
    .filter(F.col("tier_value_percent_rank_rounded") < 73)
    .select(
        "id",
        "effective_from",
        "tier_value_percent_rank_rounded",
        F.cume_dist().over(w4).alias("adjusted_tier_value_cume_dist"),
        F.rank().over(w5).alias("tier_value_dense_rank_rank"),
    )
)

result = (
    with_windows_price_tiers
    .groupBy(
        F.when(F.col("adjusted_tier_value_cume_dist") >= 0.71, "PREMIUM").otherwise("STANDARD").alias("adjusted_tier_value_cume_dist_tier"),
    )
    .agg(
        F.percentile_approx("tier_value_dense_rank_rank", [0.25, 0.5, 0.75]).alias("tier_value_dense_rank_rank_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("tier_value_dense_rank_rank").alias("total_tier_value_dense_rank_rank"),
        F.percentile_approx("adjusted_tier_value_cume_dist", [0.25, 0.5, 0.75]).alias("adjusted_tier_value_cume_dist_quartiles"),
    )
    .filter(F.col("distinct_ids") > 1)
)
