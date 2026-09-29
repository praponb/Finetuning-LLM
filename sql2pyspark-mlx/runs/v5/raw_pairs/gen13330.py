from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc())
w2 = Window.orderBy(F.col("tier_value_rank").asc_nulls_first())
w3 = Window.orderBy("tier_value_rank")
w4 = Window.orderBy(F.col("tier_value_rank").desc())
w_recent = Window.orderBy(F.col("tier_value_rank").desc_nulls_first())
w6 = Window.orderBy(F.col("tier_value_rank").asc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("discounts").alias("tier_value"))
)

scored = exploded.select("id", "effective_from", F.rank().over(w).alias("tier_value_rank"))

with_windows = (
    scored
    .filter(F.month("effective_from").isin([2, 4, 11]))
    .select(
        "effective_from",
        "tier_value_rank",
        F.percent_rank().over(w2).alias("tier_value_rank_percent_rank"),
        F.cume_dist().over(w3).alias("tier_value_rank_cume_dist"),
        F.rank().over(w4).alias("tier_value_rank_rank"),
        F.percent_rank().over(w_recent).alias("tier_value_rank_percent_rank_2"),
        F.cume_dist().over(w6).alias("tier_value_rank_cume_dist_2"),
    )
)

valid = (
    with_windows
    .filter(F.col("effective_from") > F.lit("2024-02-08").cast("date"))
    .withColumn("tier_value_rank_cume_dist_filled", F.coalesce("tier_value_rank_cume_dist", F.lit(0)))
)

base = (
    valid
    .filter((F.col("effective_from") >= F.lit("2024-03-22").cast("date")) & (F.col("tier_value_rank_percent_rank_2") >= 0.4))
    .select(
        "tier_value_rank_percent_rank",
        "tier_value_rank_cume_dist",
        "tier_value_rank_rank",
        "tier_value_rank_percent_rank_2",
        "tier_value_rank_cume_dist_2",
        F.dayofmonth("effective_from").alias("effective_from_day"),
        F.last_day("effective_from").alias("effective_from_month_end"),
    )
)

result = (
    base
    .select(
        "tier_value_rank_percent_rank",
        "tier_value_rank_rank",
        "tier_value_rank_percent_rank_2",
        "tier_value_rank_cume_dist_2",
        "effective_from_day",
    )
)
