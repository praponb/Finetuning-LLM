from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("currency_length").desc_nulls_last())
w2 = Window.partitionBy("currency")
w_running = Window.partitionBy("currency").orderBy(F.col("currency_length").desc())
w4 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_last())
w5 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

base = (
    flattened
    .filter(F.col("effective_from").between("2024-10-01", "2025-04-01"))
    .select("*", F.length("currency").alias("currency_length"))
)

scored = (
    base
    .withColumn("currency_length_dense_rank", F.dense_rank().over(w))
    .withColumn("currency_min_tier_value", F.min("tier_value").over(w2))
    .withColumn("currency_length_cume_dist", F.cume_dist().over(w_running))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w4))
)

enriched = (
    scored
    .withColumn("currency_stddev_samp_currency_length_dense_rank", F.stddev_samp("currency_length_dense_rank").over(w2))
    .withColumn("tier_value_dense_rank_2", F.dense_rank().over(w5))
)

result = (
    enriched
    .groupBy("currency")
    .agg(
        F.percentile_approx("tier_value_dense_rank_2", [0.25, 0.5, 0.75]).alias("tier_value_dense_rank_2_quartiles"),
        F.max(F.when(F.col("currency") == "THB", F.col("currency_length_dense_rank"))).alias("thb_max_currency_length_dense_rank"),
    )
    .filter(F.col("thb_max_currency_length_dense_rank") < 0)
    .orderBy("thb_max_currency_length_dense_rank")
)
