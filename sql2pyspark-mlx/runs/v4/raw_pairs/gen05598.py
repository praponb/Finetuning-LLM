from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.orderBy("tier_value")
w_ordered = Window.orderBy("tier_value_rank")
w5 = Window.orderBy(F.col("tier_value_dense_rank_2").desc())
w6 = Window.orderBy(F.col("tier_value_dense_rank_2").desc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_limits").alias("tier_value"))
)

with_windows = (
    lines
    .withColumn("tier_value_rank", F.rank().over(w))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w2))
    .withColumn("tier_value_dense_rank_2", F.dense_rank().over(w3))
)

enriched = (
    with_windows
    .withColumn("tier_value_rank_percent_rank", F.percent_rank().over(w_ordered))
    .withColumn("tier_value_dense_rank_2_percent_rank", F.percent_rank().over(w5))
    .withColumn("tier_value_rank_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w2))
    .withColumn("tier_value_dense_rank_2_dense_rank", F.dense_rank().over(w6))
)

result = (
    enriched
    .select(
        "id",
        "tier_value_rank",
        "tier_value_dense_rank",
        "tier_value_dense_rank_2",
        "tier_value_rank_percent_rank",
        "tier_value_dense_rank_2_percent_rank",
        "tier_value_rank_dense_rank",
        "tier_value_percent_rank",
        "tier_value_dense_rank_2_dense_rank",
    )
    .orderBy(F.col("tier_value_rank").desc())
)
