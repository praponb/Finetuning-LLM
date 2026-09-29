from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").asc_nulls_last())
w2 = Window.orderBy(F.col("element_filled").desc_nulls_last())
w3 = Window.orderBy("element")
w_recent = Window.orderBy(F.col("element_percent_rank").desc_nulls_last())
w5 = Window.orderBy(F.col("element_percent_rank_rank").asc_nulls_last())
w6 = Window.orderBy("element_percent_rank_rank")
w7 = Window.orderBy(F.col("element_filled_percent_rank_rounded").desc())
w8 = Window.orderBy(F.col("element_percent_rank_rank").desc())
w9 = Window.orderBy(F.col("element_filled_percent_rank_rounded").desc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_prices").alias("element"))
)

filtered = (
    lines
    .filter(F.col("effective_from") > F.lit("2024-05-05").cast("date"))
    .select(
        "*",
        F.date_add("effective_from", 14).alias("effective_from_plus_14d"),
        F.coalesce("element", F.lit(0)).alias("element_filled"),
    )
)

metrics = (
    filtered
    .select(
        "effective_from_plus_14d",
        "element_filled",
        F.percent_rank().over(w).alias("element_percent_rank"),
        F.percent_rank().over(w2).alias("element_filled_percent_rank"),
        F.rank().over(w3).alias("element_rank"),
    )
)

metrics_price_tiers = (
    metrics
    .filter(F.col("element_rank") > 5)
    .select(
        "element_percent_rank",
        "element_filled_percent_rank",
        F.rank().over(w_recent).alias("element_percent_rank_rank"),
    )
)

valid = (
    metrics_price_tiers
    .filter(F.col("element_percent_rank_rank") >= 6)
    .select(
        "*",
        F.round("element_filled_percent_rank", 1).alias("element_filled_percent_rank_rounded"),
    )
)

scored = (
    valid
    .select(
        "element_percent_rank",
        "element_filled_percent_rank",
        F.cume_dist().over(w5).alias("element_percent_rank_rank_cume_dist"),
        F.dense_rank().over(w6).alias("element_percent_rank_rank_dense_rank"),
        F.cume_dist().over(w7).alias("element_filled_percent_rank_rounded_cume_dist"),
        F.percent_rank().over(w8).alias("element_percent_rank_rank_percent_rank"),
        F.dense_rank().over(w9).alias("element_filled_percent_rank_rounded_dense_rank"),
    )
)

result = (
    scored
    .select(
        "element_percent_rank",
        "element_filled_percent_rank",
        "element_percent_rank_rank_cume_dist",
        "element_filled_percent_rank_rounded_cume_dist",
        "element_percent_rank_rank_percent_rank",
        "element_filled_percent_rank_rounded_dense_rank",
    )
    .orderBy(
        F.col("element_filled_percent_rank_rounded_cume_dist").asc_nulls_last(),
        "element_percent_rank_rank_percent_rank",
    )
)
