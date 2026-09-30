from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w_recent = Window.partitionBy("plan")
w4 = Window.partitionBy("currency_sum_currency_min_tier_value_times_tier_value_tier").orderBy(F.col("starter_plan_avg_tier_value_percent_rank_per_tier_value").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

enriched = (
    flattened
    .select(
        "*",
        F.min("tier_value").over(w).alias("currency_min_tier_value"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
    )
)

filtered = (
    enriched
    .filter(F.col("tier_value_percent_rank").between(0.23, 0.77))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "currency_min_tier_value",
        "tier_value_percent_rank",
        (F.col("currency_min_tier_value") * F.col("tier_value")).alias("currency_min_tier_value_times_tier_value"),
        (F.col("tier_value_percent_rank") / F.nullif("tier_value", F.lit(0))).alias("tier_value_percent_rank_per_tier_value"),
    )
)

matched_keys = (
    filtered
    .select("currency", "id")
    .subtract(
        df_price_tiers
        .filter(~F.col("currency").isin(["EUR", "THB"]))
        .select("currency", "id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

remaining_keys = (
    verified
    .select("id")
    .subtract(
        df_price_tiers
        .filter(F.col("effective_from") >= "2024-04-01")
        .select("id")
    )
)

kept_rows = (
    verified
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .withColumn("plan_avg_tier_value_percent_rank_per_tier_value", F.avg("tier_value_percent_rank_per_tier_value").over(w_recent))
    .withColumn("plan_avg_tier_value_percent_rank_per_tier_value_2", F.avg("tier_value_percent_rank_per_tier_value").over(w_recent))
    .withColumn("currency_sum_currency_min_tier_value_times_tier_value", F.sum("currency_min_tier_value_times_tier_value").over(w))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("currency_sum_currency_min_tier_value_times_tier_value") >= 402, "PREMIUM").otherwise("STANDARD").alias("currency_sum_currency_min_tier_value_times_tier_value_tier"),
        "plan",
    )
    .agg(
        F.round(F.avg("tier_value_percent_rank_per_tier_value"), 2).alias("avg_tier_value_percent_rank_per_tier_value"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("plan") == "basic", F.col("currency_min_tier_value_times_tier_value")).otherwise(0)).alias("basic_currency_min_tier_value_times_tier_value"),
        F.min("tier_value_percent_rank").alias("min_tier_value_percent_rank"),
        F.sum(F.when(F.col("plan") == "starter", F.col("plan_avg_tier_value_percent_rank_per_tier_value")).otherwise(0)).alias("starter_plan_avg_tier_value_percent_rank_per_tier_value"),
    )
    .filter(F.col("distinct_ids") > 1)
    .withColumn("starter_plan_avg_tier_value_percent_rank_per_tier_value_rank", F.rank().over(w4))
)
