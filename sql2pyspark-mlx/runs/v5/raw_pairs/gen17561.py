from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_last())
w3 = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value").desc_nulls_last())
w4 = Window.partitionBy("plan")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

with_windows = (
    flattened
    .select(
        "id",
        "plan",
        F.min("tier_value").over(w_ordered).alias("currency_min_tier_value"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
        F.max("tier_value").over(w_ordered).alias("currency_max_tier_value"),
        F.dense_rank().over(w3).alias("tier_value_dense_rank"),
    )
)

kept_keys = (
    with_windows
    .select("id")
    .intersect(
        df_price_tiers
        .filter(F.col("plan").isin(["starter", "pro", "enterprise"]))
        .select("id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows_price_tiers = (
    verified
    .filter(F.col("tier_value_dense_rank") == 4)
    .select(
        "*",
        F.count("tier_value_dense_rank").over(w4).alias("plan_count_tier_value_dense_rank"),
    )
)

matched_keys = (
    with_windows_price_tiers
    .select("plan", "id")
    .intersectAll(
        df_price_tiers
        .filter(F.col("effective_from") < "2024-12-31")
        .select("plan", "id")
    )
)

reconciled = (
    with_windows_price_tiers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "tier_value_cume_dist",
        "currency_max_tier_value",
        "tier_value_dense_rank",
        "plan_count_tier_value_dense_rank",
    )
    .orderBy("tier_value_cume_dist")
)
