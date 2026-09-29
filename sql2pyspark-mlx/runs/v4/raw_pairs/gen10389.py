from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value_filled").desc_nulls_first())
w2 = Window.orderBy(F.col("tier_value_filled").asc_nulls_last())
w3 = Window.orderBy(F.col("tier_value").asc_nulls_last())
w4 = Window.orderBy(F.col("tier_value_cume_dist").desc_nulls_first())
w5 = Window.orderBy(F.col("effective_from_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.orderBy("tier_value_sum_90d")
w_ordered = Window.orderBy(F.col("tier_value").desc())
w8 = Window.orderBy(F.col("effective_from_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w9 = Window.orderBy(F.col("days_since_effective_from").desc())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_prices").alias("tier_value"))
)

valid = (
    lines
    .filter(F.col("effective_from").between("2023-08-21", "2024-02-05"))
    .withColumn("tier_value_filled", F.coalesce("tier_value", F.lit(0)))
)

base = (
    valid
    .filter(F.col("effective_from") > F.lit("2024-06-26").cast("date"))
    .select("*", F.date_trunc("week", "effective_from").alias("effective_from_period"))
)

enriched = (
    base
    .select(
        "*",
        F.percent_rank().over(w).alias("tier_value_filled_percent_rank"),
        F.percent_rank().over(w2).alias("tier_value_filled_percent_rank_2"),
        F.cume_dist().over(w3).alias("tier_value_cume_dist"),
    )
)

enriched_price_tiers = (
    enriched
    .withColumn("tier_value_cume_dist_percent_rank", F.percent_rank().over(w4))
    .withColumn("tier_value_sum_90d", F.sum("tier_value").over(w5))
)

cleaned = (
    enriched_price_tiers
    .filter((F.col("effective_from") > F.lit("2024-09-18").cast("date")) & (F.col("tier_value") > 72))
    .select(
        "effective_from",
        "tier_value",
        "tier_value_filled",
        "effective_from_period",
        "tier_value_filled_percent_rank",
        "tier_value_cume_dist",
        "tier_value_sum_90d",
        F.datediff(F.current_date(), "effective_from").alias("days_since_effective_from"),
    )
)

enriched_price_tiers_2 = (
    cleaned
    .select(
        "*",
        F.rank().over(w6).alias("tier_value_sum_90d_rank"),
        F.percent_rank().over(w_ordered).alias("tier_value_percent_rank"),
        F.count("days_since_effective_from").over(w8).alias("days_since_effective_from_count_30d"),
        F.percent_rank().over(w9).alias("days_since_effective_from_percent_rank"),
        F.dense_rank().over(w2).alias("tier_value_filled_dense_rank"),
    )
)

result = (
    enriched_price_tiers_2
    .groupBy(
        F.when(F.col("tier_value_filled_percent_rank") >= 0.77, "HIGH").otherwise("LOW").alias("tier_value_filled_percent_rank_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("effective_from_period")) - F.unix_timestamp(F.min("effective_from_period"))) / 60).alias("duration_min"),
        F.percentile_approx("tier_value_percent_rank", 0.5).alias("tier_value_percent_rank_p50"),
        F.round(F.avg("tier_value_filled"), 2).alias("avg_tier_value_filled"),
    )
    .orderBy(F.col("avg_tier_value_filled").desc())
)
