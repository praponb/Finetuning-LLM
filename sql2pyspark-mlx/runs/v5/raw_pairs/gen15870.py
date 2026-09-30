from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

scored = lines.select("*", F.dense_rank().over(w).alias("tier_value_dense_rank"))

valid = (
    scored
    .filter(F.col("currency") == "THB")
    .withColumn("plan_lower", F.lower("plan"))
)

valid_price_tiers = (
    valid
    .filter(F.col("tier_value") == 68)
    .select(
        "id",
        "plan",
        "tier_value",
        "tier_value_dense_rank",
        "plan_lower",
        F.round((F.col("tier_value_dense_rank") * 1.1), 2).alias("adjusted_tier_value_dense_rank"),
        F.ceil("tier_value").alias("tier_value_whole"),
    )
)

base = (
    valid_price_tiers
    .filter(F.col("tier_value") > 42)
    .select(
        "id",
        "plan",
        "tier_value_dense_rank",
        "adjusted_tier_value_dense_rank",
        F.ceil("adjusted_tier_value_dense_rank").alias("adjusted_tier_value_dense_rank_whole"),
        (F.col("tier_value") - F.col("adjusted_tier_value_dense_rank")).alias("tier_value_minus_adjusted_tier_value_dense_rank"),
    )
)

base_price_tiers = (
    base
    .filter(F.col("adjusted_tier_value_dense_rank") > 32)
    .select(
        "id",
        "plan",
        "tier_value_dense_rank",
        "adjusted_tier_value_dense_rank_whole",
        "tier_value_minus_adjusted_tier_value_dense_rank",
        (F.col("adjusted_tier_value_dense_rank_whole") - F.col("tier_value_minus_adjusted_tier_value_dense_rank")).alias("adjusted_tier_value_dense_rank_whole_minus_tier_value_minus_adjusted_tier_value_dense_rank"),
        (F.col("adjusted_tier_value_dense_rank") / F.nullif("tier_value_minus_adjusted_tier_value_dense_rank", F.lit(0))).alias("adjusted_tier_value_dense_rank_per_tier_value_minus_adjusted_tier_value_dense_rank"),
    )
)

remaining_keys = (
    base_price_tiers
    .select("id", "plan")
    .intersect(
        df_price_tiers
        .filter(F.col("currency").isin(["USD", "EUR"]))
        .select("id", "plan")
    )
)

kept_rows = (
    base_price_tiers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

grouped = (
    kept_rows
    .withColumn("tier_value_minus_adjusted_tier_value_dense_rank_tier", F.when(F.col("tier_value_minus_adjusted_tier_value_dense_rank") >= 76, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["plan", "tier_value_minus_adjusted_tier_value_dense_rank_tier"], ["plan"]],
        "plan",
        "tier_value_minus_adjusted_tier_value_dense_rank_tier",
    )
    .agg(
        F.count_if(F.col("plan") == "starter").alias("starter_events"),
        F.count_if(F.col("plan") == "starter").alias("starter_events_2"),
        F.round(F.sum("adjusted_tier_value_dense_rank_per_tier_value_minus_adjusted_tier_value_dense_rank") / F.nullif(F.count(F.when(F.col("plan") == "pro", 1)), F.lit(0)), 2).alias("adjusted_tier_value_dense_rank_per_tier_value_minus_adjusted_tier_value_dense_rank_per_pro"),
        F.round(F.sum("tier_value_minus_adjusted_tier_value_dense_rank") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("tier_value_minus_adjusted_tier_value_dense_rank_per_basic"),
    )
)

result = (
    grouped
    .select(
        "tier_value_minus_adjusted_tier_value_dense_rank_tier",
        "starter_events",
        "adjusted_tier_value_dense_rank_per_tier_value_minus_adjusted_tier_value_dense_rank_per_pro",
        "tier_value_minus_adjusted_tier_value_dense_rank_per_basic",
    )
    .orderBy(
        "starter_events",
        F.col("adjusted_tier_value_dense_rank_per_tier_value_minus_adjusted_tier_value_dense_rank_per_pro").desc(),
    )
)
