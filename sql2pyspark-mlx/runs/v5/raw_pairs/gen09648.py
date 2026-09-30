from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w_grp = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value_filled_rounded").desc_nulls_last())
w3 = Window.orderBy(F.col("tier_value_filled_rounded").asc_nulls_last())
w4 = Window.orderBy(F.col("tier_value_filled_rounded").asc_nulls_first())
w5 = Window.partitionBy("plan").orderBy(F.col("tier_value_filled_rounded_rank").asc_nulls_last())
w_running = Window.partitionBy("plan")

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

valid = (
    exploded
    .filter(F.col("effective_from") > F.lit("2024-01-23").cast("date"))
    .select("*", F.coalesce("tier_value", F.lit(0)).alias("tier_value_filled"))
)

valid_price_tiers = (
    valid
    .filter(F.col("effective_from") >= F.lit("2024-05-15").cast("date"))
    .withColumn("tier_value_filled_rounded", F.round("tier_value_filled", 2))
)

scored = (
    valid_price_tiers
    .filter(F.col("currency").isin(["USD", "THB", "EUR"]))
    .withColumn("currency_avg_tier_value_filled_rounded", F.avg("tier_value_filled_rounded").over(w))
    .withColumn("tier_value_filled_rounded_rank", F.rank().over(w_grp))
    .withColumn("tier_value_filled_rounded_cume_dist", F.cume_dist().over(w3))
)

filtered = (
    scored
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-04-26")))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_value",
        "tier_value_filled_rounded",
        "tier_value_filled_rounded_rank",
        "tier_value_filled_rounded_cume_dist",
        F.substring("plan", 1, 3).alias("plan_prefix"),
        F.round("tier_value_filled_rounded_cume_dist", 0).alias("tier_value_filled_rounded_cume_dist_rounded"),
    )
)

scored_price_tiers = (
    filtered
    .select(
        "*",
        F.cume_dist().over(w4).alias("tier_value_filled_rounded_cume_dist_2"),
        F.rank().over(w5).alias("tier_value_filled_rounded_rank_rank"),
        F.stddev_samp("tier_value").over(w_running).alias("plan_stddev_samp_tier_value"),
        F.stddev_samp("tier_value_filled_rounded_cume_dist").over(w_running).alias("plan_stddev_samp_tier_value_filled_rounded_cume_dist"),
    )
)

result = (
    scored_price_tiers
    .groupBy(
        F.when(F.col("plan_stddev_samp_tier_value") >= 82, "HIGH").otherwise("LOW").alias("plan_stddev_samp_tier_value_tier"),
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
    )
    .agg(
        F.round(F.sum("plan_stddev_samp_tier_value") / F.nullif(F.count(F.when(F.col("currency") == "EUR", 1)), F.lit(0)), 2).alias("plan_stddev_samp_tier_value_per_eur"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.avg("tier_value_filled_rounded_rank").alias("avg_tier_value_filled_rounded_rank"),
    )
)
