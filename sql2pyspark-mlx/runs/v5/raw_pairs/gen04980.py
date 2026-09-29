from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("tier_value")
w2 = Window.partitionBy("currency").orderBy("tier_value_dense_rank")
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value_dense_rank").desc())
w4 = Window.partitionBy("currency")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

with_windows = (
    flattened
    .select(
        "effective_from",
        "currency",
        "tier_value",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
    )
)

filtered = (
    with_windows
    .filter(F.col("tier_value_dense_rank") == 7)
    .withColumn("currency_length", F.length("currency"))
)

scored = (
    filtered
    .filter(F.year("effective_from") == 2024)
    .select(
        "effective_from",
        "currency",
        "tier_value",
        "tier_value_dense_rank",
        "currency_length",
        F.percent_rank().over(w2).alias("tier_value_dense_rank_percent_rank"),
        F.dense_rank().over(w3).alias("tier_value_dense_rank_dense_rank"),
        F.min("tier_value_dense_rank").over(w4).alias("currency_min_tier_value_dense_rank"),
    )
)

result = (
    scored
    .groupBy(F.date_trunc("WEEK", "effective_from").alias("week_start"), "currency")
    .agg(
        F.percentile_approx("tier_value_dense_rank_percent_rank", 0.5).alias("tier_value_dense_rank_percent_rank_p50"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
    )
    .orderBy(F.col("tier_value_dense_rank_percent_rank_p50").desc_nulls_last())
)
