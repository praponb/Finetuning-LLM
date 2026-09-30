from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("element")
w_part = Window.orderBy(F.col("element").desc_nulls_first())
w3 = Window.orderBy(F.col("element").desc())
w4 = Window.orderBy(F.col("element_rank").desc())
w5 = Window.orderBy(F.col("element_percent_rank").asc_nulls_last())
w6 = Window.orderBy(F.col("element_rank").desc_nulls_last())
w7 = Window.orderBy(F.col("element").asc_nulls_first())
w8 = Window.orderBy(F.col("element_rank_dense_rank").desc_nulls_first())
w9 = Window.orderBy(F.col("element_percent_rank_2").desc_nulls_first())

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_prices").alias("element"))
)

metrics = (
    exploded
    .select(
        "*",
        F.dense_rank().over(w).alias("element_dense_rank"),
        F.rank().over(w_part).alias("element_rank"),
        F.percent_rank().over(w3).alias("element_percent_rank"),
    )
)

metrics_price_tiers = (
    metrics
    .withColumn("element_rank_dense_rank", F.dense_rank().over(w4))
    .withColumn("element_percent_rank_dense_rank", F.dense_rank().over(w5))
    .withColumn("element_rank_rank", F.rank().over(w6))
    .withColumn("element_percent_rank_2", F.percent_rank().over(w7))
)

with_windows = (
    metrics_price_tiers
    .withColumn("element_rank_dense_rank_percent_rank", F.percent_rank().over(w8))
    .withColumn("element_percent_rank_2_dense_rank", F.dense_rank().over(w9))
)

result = (
    with_windows
    .select(
        "element",
        "element_dense_rank",
        "element_rank",
        "element_rank_dense_rank",
        "element_percent_rank_dense_rank",
        "element_rank_rank",
        "element_rank_dense_rank_percent_rank",
    )
    .orderBy(F.col("element_dense_rank").asc_nulls_last(), "element_rank_rank")
)
