from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc())
w2 = Window.partitionBy("plan").orderBy("tier_value")
w3 = Window.partitionBy("plan")
w4 = Window.orderBy(F.col("tier_value").desc_nulls_last())
w5 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

with_windows = (
    flattened
    .filter(F.col("plan") == "enterprise")
    .select(
        "id",
        "plan",
        "tier_value",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
    )
)

enriched = with_windows.select("*", F.dense_rank().over(w2).alias("tier_value_dense_rank_2"))

kept_keys = (
    enriched
    .select("id")
    .intersectAll(
        df_price_tiers
        .filter(F.col("currency").isin(["USD", "EUR", "THB"]))
        .select("id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows_price_tiers = (
    kept_rows
    .select(
        "*",
        F.min("tier_value_dense_rank").over(w3).alias("plan_min_tier_value_dense_rank"),
        F.rank().over(w4).alias("tier_value_rank"),
        F.rank().over(w5).alias("tier_value_rank_2"),
    )
)

cleaned = (
    with_windows_price_tiers
    .filter(F.col("plan") == "basic")
    .select(
        "plan",
        "tier_value_dense_rank_2",
        "plan_min_tier_value_dense_rank",
        "tier_value_rank",
        "tier_value_rank_2",
        F.col("tier_value").cast("double").alias("tier_value_num"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("plan_min_tier_value_dense_rank") >= 9, "LARGE").otherwise("SMALL").alias("plan_min_tier_value_dense_rank_tier"),
        "plan",
        F.when(F.col("tier_value_dense_rank_2") >= 7, "PREMIUM").otherwise("STANDARD").alias("tier_value_dense_rank_2_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_rows"),
        F.percentile_approx("tier_value_rank_2", 0.9).alias("tier_value_rank_2_p90"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("plan_min_tier_value_dense_rank")).otherwise(0)).alias("enterprise_plan_min_tier_value_dense_rank"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_rows_2"),
        F.round(F.sum("tier_value_dense_rank_2") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("tier_value_dense_rank_2_per_enterprise"),
    )
)
