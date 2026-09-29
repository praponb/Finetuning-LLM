from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("plan", "currency").orderBy("tier_value")
w_recent_2 = Window.orderBy(F.col("tier_value").desc())
w3 = Window.partitionBy("plan").orderBy(F.col("currency_length").asc_nulls_first())
w4 = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value").asc_nulls_last())
w5 = Window.orderBy(F.col("currency_length").asc_nulls_first())
w6 = Window.orderBy(F.col("currency_length").desc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

with_windows = (
    flattened
    .filter(F.col("plan") == "basic")
    .select(
        "*",
        F.rank().over(w_recent).alias("tier_value_rank"),
        F.percent_rank().over(w_recent_2).alias("tier_value_percent_rank"),
    )
)

base = (
    with_windows
    .filter(F.col("effective_from") > F.lit("2024-04-18").cast("date"))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_value",
        "tier_value_rank",
        "tier_value_percent_rank",
        F.length("currency").alias("currency_length"),
    )
)

matched_keys = (
    base
    .select("plan", "currency")
    .exceptAll(
        df_price_tiers
        .filter(F.col("plan") != "pro")
        .select("plan", "currency")
    )
)

reconciled = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "id",
        "effective_from",
        "tier_value",
        "tier_value_rank",
        "tier_value_percent_rank",
        "currency_length",
        F.percent_rank().over(w3).alias("currency_length_percent_rank"),
        F.cume_dist().over(w4).alias("tier_value_cume_dist"),
    )
)

with_windows_price_tiers = (
    enriched
    .select(
        "id",
        "tier_value",
        "tier_value_percent_rank",
        "currency_length_percent_rank",
        "tier_value_cume_dist",
        F.percent_rank().over(w5).alias("currency_length_percent_rank_2"),
        F.percent_rank().over(w6).alias("currency_length_percent_rank_3"),
    )
)

grouped = (
    with_windows_price_tiers
    .groupBy(
        F.when(F.col("currency_length_percent_rank_3") >= 0.74, "PREMIUM").otherwise("STANDARD").alias("currency_length_percent_rank_3_tier"),
    )
    .agg(
        F.percentile_approx("tier_value_percent_rank", [0.25, 0.5, 0.75]).alias("tier_value_percent_rank_quartiles"),
        F.percentile_approx("currency_length_percent_rank", [0.25, 0.5, 0.75]).alias("currency_length_percent_rank_quartiles"),
        F.percentile_approx("currency_length_percent_rank_2", [0.25, 0.5, 0.75]).alias("currency_length_percent_rank_2_quartiles"),
        F.percentile_approx("tier_value_cume_dist", [0.25, 0.5, 0.75]).alias("tier_value_cume_dist_quartiles"),
    )
)

result = (
    grouped
    .select(
        "currency_length_percent_rank_3_tier",
        "currency_length_percent_rank_quartiles",
        "tier_value_cume_dist_quartiles",
    )
    .orderBy(F.col("currency_length_percent_rank_3_tier").asc_nulls_first())
)
