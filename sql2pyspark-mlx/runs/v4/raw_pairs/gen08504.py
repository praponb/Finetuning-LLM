from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc())
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w_ordered = Window.partitionBy("currency")
w4 = Window.partitionBy("currency").orderBy(F.col("tier_value_dense_rank").desc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "currency", F.explode("tier_prices").alias("tier_value"))
)

matched_keys = (
    flattened
    .select("currency", "id")
    .exceptAll(
        df_price_tiers
        .filter(F.col("plan").isin(["basic", "enterprise"]))
        .select("currency", "id")
    )
)

kept_rows = (
    flattened
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "*",
        F.cume_dist().over(w).alias("tier_value_cume_dist"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.stddev_samp("tier_value_cume_dist").over(w_ordered).alias("currency_stddev_samp_tier_value_cume_dist"),
        F.max("tier_value_cume_dist").over(w_ordered).alias("currency_max_tier_value_cume_dist"),
        F.cume_dist().over(w).alias("tier_value_cume_dist_2"),
        F.dense_rank().over(w4).alias("tier_value_dense_rank_dense_rank"),
        F.max("tier_value").over(w_ordered).alias("currency_max_tier_value"),
    )
)

result = (
    with_windows
    .rollup(
        F.when(F.col("currency_max_tier_value") >= 65, "HIGH").otherwise("LOW").alias("currency_max_tier_value_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value"),
    )
    .orderBy(F.col("event_count").desc_nulls_last())
)
