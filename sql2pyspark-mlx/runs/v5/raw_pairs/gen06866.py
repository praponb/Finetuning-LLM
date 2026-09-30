from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("tier_value").desc_nulls_first())
w_ordered = Window.orderBy(F.col("tier_value").desc())
w_recent = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_first())
w4 = Window.orderBy(F.col("tier_value_dense_rank").asc_nulls_last())
w5 = Window.partitionBy("plan")
w6 = Window.orderBy("tier_value_dense_rank")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

base = (
    flattened
    .filter((F.col("effective_from") >= F.lit("2024-04-14").cast("date")) & (F.col("plan") == "pro"))
    .select("*", F.upper("plan").alias("plan_upper"))
)

with_windows = (
    base
    .filter(F.col("currency") == "EUR")
    .select(
        "plan",
        "effective_from",
        "plan_upper",
        F.rank().over(w_grp).alias("tier_value_rank"),
        F.dense_rank().over(w_ordered).alias("tier_value_dense_rank"),
        F.rank().over(w_recent).alias("tier_value_rank_2"),
    )
)

with_windows_price_tiers = (
    with_windows
    .select(
        "*",
        F.dense_rank().over(w4).alias("tier_value_dense_rank_dense_rank"),
        F.avg("tier_value_rank").over(w5).alias("plan_avg_tier_value_rank"),
        F.cume_dist().over(w6).alias("tier_value_dense_rank_cume_dist"),
    )
)

cleaned = (
    with_windows_price_tiers
    .filter(F.col("tier_value_rank").between(4, 8))
    .select(
        "plan",
        "effective_from",
        "plan_upper",
        "tier_value_dense_rank",
        "tier_value_rank_2",
        "tier_value_dense_rank_dense_rank",
        "plan_avg_tier_value_rank",
        "tier_value_dense_rank_cume_dist",
        (F.col("tier_value_rank") * F.col("tier_value_dense_rank")).alias("tier_value_rank_times_tier_value_dense_rank"),
    )
)

filtered = (
    cleaned
    .filter(F.col("tier_value_dense_rank_cume_dist").isNotNull())
    .withColumn("tier_value_dense_rank_scaled", F.round((F.col("tier_value_dense_rank") * 0.85), 2))
)

cleaned_price_tiers = (
    filtered
    .filter(F.col("effective_from") > F.lit("2024-05-24").cast("date"))
    .select(
        "*",
        F.round("tier_value_dense_rank", 2).alias("tier_value_dense_rank_rounded"),
    )
)

result = (
    cleaned_price_tiers
    .select(
        "plan",
        "effective_from",
        "plan_upper",
        "tier_value_dense_rank",
        "tier_value_rank_2",
        "plan_avg_tier_value_rank",
        "tier_value_dense_rank_cume_dist",
        "tier_value_dense_rank_scaled",
        "tier_value_dense_rank_rounded",
    )
)
