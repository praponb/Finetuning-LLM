from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("tier_value")
w2 = Window.orderBy(F.col("tier_value").asc_nulls_first())
w_recent = Window.orderBy(F.col("tier_value").asc_nulls_last())
w4 = Window.orderBy(F.col("tier_value_cume_dist").desc_nulls_last())
w5 = Window.orderBy(F.col("tier_value_cume_dist").desc_nulls_first())
w6 = Window.orderBy(F.col("tier_value_dense_rank").desc())
w7 = Window.orderBy(F.col("tier_value_dense_rank_cume_dist").desc_nulls_last())
w8 = Window.orderBy(F.col("tier_value").desc_nulls_last())
w9 = Window.orderBy("tier_value_rank")
w10 = Window.orderBy("tier_value_cume_dist_percent_rank")

flattened = df_price_tiers.select("id", F.explode("discounts").alias("tier_value"))

scored = (
    flattened
    .select(
        "*",
        F.rank().over(w_running).alias("tier_value_rank"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
        F.dense_rank().over(w_recent).alias("tier_value_dense_rank"),
    )
)

matched_keys = (
    scored
    .select("id")
    .subtract(
        df_price_tiers
        .filter(F.col("currency").isin(["THB", "EUR", "USD"]))
        .select("id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .withColumn("tier_value_cume_dist_percent_rank", F.percent_rank().over(w4))
    .withColumn("tier_value_cume_dist_rank", F.rank().over(w5))
    .withColumn("tier_value_dense_rank_cume_dist", F.cume_dist().over(w6))
)

enriched = (
    with_windows
    .withColumn("tier_value_dense_rank_cume_dist_dense_rank", F.dense_rank().over(w7))
    .withColumn("tier_value_dense_rank_2", F.dense_rank().over(w8))
    .withColumn("tier_value_rank_rank", F.rank().over(w9))
    .withColumn("tier_value_rank_2", F.rank().over(w_recent))
    .withColumn("tier_value_cume_dist_percent_rank_dense_rank", F.dense_rank().over(w10))
)

cleaned = (
    enriched
    .filter(F.col("tier_value_cume_dist") >= 0.68)
    .select(
        "id",
        "tier_value",
        "tier_value_cume_dist",
        "tier_value_dense_rank",
        "tier_value_cume_dist_percent_rank",
        "tier_value_rank_rank",
        "tier_value_rank_2",
        "tier_value_cume_dist_percent_rank_dense_rank",
        F.round((F.col("tier_value_dense_rank_2") * 0.9), 2).alias("adjusted_tier_value_dense_rank_2"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "tier_value",
        "tier_value_cume_dist",
        "tier_value_cume_dist_percent_rank",
        "tier_value_rank_rank",
        "adjusted_tier_value_dense_rank_2",
    )
)
