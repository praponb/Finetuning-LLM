from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())
w3 = Window.partitionBy("currency")
w4 = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value_rounded").desc_nulls_first())
w5 = Window.orderBy(F.col("tier_value").asc_nulls_first())
w6 = Window.orderBy(F.col("tier_value_cume_dist").asc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("tier_value"),
    )
)

cleaned = (
    lines
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-01-24")))
    .select("*", F.round("tier_value", 0).alias("tier_value_rounded"))
)

scored = (
    cleaned
    .filter(F.col("effective_from") <= "2024-03-01")
    .select(
        "currency",
        "tier_value",
        "tier_value_rounded",
        F.sum("tier_value").over(w).alias("plan_sum_tier_value"),
        F.avg("tier_value_rounded").over(w).alias("plan_avg_tier_value_rounded"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
        F.max("tier_value").over(w3).alias("currency_max_tier_value"),
        F.cume_dist().over(w4).alias("tier_value_rounded_cume_dist"),
    )
)

metrics = (
    scored
    .select(
        "tier_value",
        "plan_avg_tier_value_rounded",
        "tier_value_cume_dist",
        F.count("tier_value").over(w3).alias("currency_count_tier_value"),
        F.avg("tier_value_rounded").over(w3).alias("currency_avg_tier_value_rounded"),
    )
)

with_windows = (
    metrics
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w5))
    .withColumn("tier_value_cume_dist_rank", F.rank().over(w6))
)

valid = (
    with_windows
    .filter(F.col("tier_value_cume_dist_rank").between(5, 7))
    .select(
        "tier_value",
        "plan_avg_tier_value_rounded",
        "currency_count_tier_value",
        "currency_avg_tier_value_rounded",
        "tier_value_percent_rank",
        "tier_value_cume_dist_rank",
        (F.col("plan_avg_tier_value_rounded") + F.col("tier_value_cume_dist")).alias("plan_avg_tier_value_rounded_plus_tier_value_cume_dist"),
        F.round("tier_value_cume_dist_rank", 0).alias("tier_value_cume_dist_rank_rounded"),
    )
)

cleaned_price_tiers = (
    valid
    .filter(F.col("tier_value") > 27)
    .select(
        "tier_value",
        "plan_avg_tier_value_rounded",
        "currency_avg_tier_value_rounded",
        "tier_value_percent_rank",
        "tier_value_cume_dist_rank",
        "plan_avg_tier_value_rounded_plus_tier_value_cume_dist",
        "tier_value_cume_dist_rank_rounded",
        F.col("tier_value_cume_dist_rank").cast("double").alias("tier_value_cume_dist_rank_num"),
    )
)

result = (
    cleaned_price_tiers
    .groupBy(
        F.when(F.col("tier_value_cume_dist_rank_rounded") >= 62, "PREMIUM").otherwise("STANDARD").alias("tier_value_cume_dist_rank_rounded_tier"),
        F.when(F.col("tier_value") >= 72, "LARGE").otherwise("SMALL").alias("tier_value_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("plan_avg_tier_value_rounded_plus_tier_value_cume_dist"), 2).alias("avg_plan_avg_tier_value_rounded_plus_tier_value_cume_dist"),
        F.percentile_approx("tier_value_cume_dist_rank", [0.25, 0.5, 0.75]).alias("tier_value_cume_dist_rank_quartiles"),
    )
    .orderBy(F.col("avg_plan_avg_tier_value_rounded_plus_tier_value_cume_dist").desc())
)
