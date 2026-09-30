from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_first())
w2 = Window.orderBy("tier_value")
w3 = Window.orderBy(F.col("tier_value").desc_nulls_last())
w4 = Window.orderBy(F.col("tier_value").asc_nulls_last())
w5 = Window.orderBy(F.col("tier_value_percent_rank").desc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_limits").alias("tier_value"))
)

kept_keys = (
    exploded
    .select("id")
    .intersect(
        df_price_tiers
        .filter(F.col("currency") == "USD")
        .select("id")
    )
)

verified = (
    exploded
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "effective_from",
        "tier_value",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
        F.rank().over(w3).alias("tier_value_rank"),
        F.percent_rank().over(w3).alias("tier_value_percent_rank_2"),
        F.dense_rank().over(w4).alias("tier_value_dense_rank_2"),
    )
)

enriched = (
    with_windows
    .withColumn("tier_value_rank_2", F.rank().over(w))
    .withColumn("tier_value_percent_rank_rank", F.rank().over(w5))
)

result = (
    enriched
    .select(
        "effective_from",
        "tier_value",
        "tier_value_percent_rank",
        "tier_value_rank",
        "tier_value_percent_rank_2",
        "tier_value_rank_2",
        "tier_value_percent_rank_rank",
    )
    .orderBy(F.col("tier_value_percent_rank").desc_nulls_last())
)
