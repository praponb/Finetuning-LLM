from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc())
w_ordered = Window.orderBy(F.col("tier_value_dense_rank").desc_nulls_last())
w3 = Window.orderBy(F.col("tier_value_dense_rank").asc_nulls_last())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_prices").alias("tier_value"),
    )
)

kept_keys = (
    flattened
    .select("id", "plan")
    .intersectAll(
        df_price_tiers
        .filter(~F.col("currency").isin(["USD", "EUR"]))
        .select("id", "plan")
    )
)

verified = (
    flattened
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .filter(F.col("tier_value").between(56, 63))
    .select(
        "id",
        "effective_from",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
    )
)

cleaned = (
    enriched
    .filter(F.col("effective_from") >= "2024-04-01")
    .select(
        "*",
        F.date_sub("effective_from", 58).alias("effective_from_minus_58d"),
        F.add_months("effective_from", 3).alias("effective_from_plus_months"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "effective_from",
        "tier_value_dense_rank",
        "effective_from_plus_months",
        F.percent_rank().over(w_ordered).alias("tier_value_dense_rank_percent_rank"),
        F.percent_rank().over(w_ordered).alias("tier_value_dense_rank_percent_rank_2"),
        F.rank().over(w_ordered).alias("tier_value_dense_rank_rank"),
        F.cume_dist().over(w_ordered).alias("tier_value_dense_rank_cume_dist"),
        F.rank().over(w3).alias("tier_value_dense_rank_rank_2"),
    )
)

matched_keys = (
    metrics
    .select("id")
    .exceptAll(
        df_price_tiers
        .filter(F.col("plan") != "pro")
        .select("id")
    )
)

verified_price_tiers = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified_price_tiers
    .groupBy(
        F.date_trunc("QUARTER", "effective_from_plus_months").alias("quarter_start"),
    )
    .agg(
        F.percentile_approx("tier_value_dense_rank_rank_2", 0.95).alias("tier_value_dense_rank_rank_2_p95"),
        F.max("tier_value_dense_rank_cume_dist").alias("max_tier_value_dense_rank_cume_dist"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("tier_value_dense_rank_percent_rank", [0.25, 0.5, 0.75]).alias("tier_value_dense_rank_percent_rank_quartiles"),
    )
    .orderBy(F.col("distinct_ids_2").asc_nulls_first())
)
