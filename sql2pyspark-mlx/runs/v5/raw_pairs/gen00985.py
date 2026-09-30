from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc_nulls_last())
w2 = Window.partitionBy("currency")
w3 = Window.orderBy(F.col("element").desc_nulls_first())
w4 = Window.orderBy(F.col("element").desc())
w5 = Window.orderBy(F.col("element_dense_rank_3").desc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

metrics = lines.withColumn("element_dense_rank", F.dense_rank().over(w))

metrics_price_tiers = (
    metrics
    .select(
        "currency",
        "element",
        "element_dense_rank",
        F.avg("element_dense_rank").over(w2).alias("currency_avg_element_dense_rank"),
        F.avg("element_dense_rank").over(w2).alias("currency_avg_element_dense_rank_2"),
        F.dense_rank().over(w3).alias("element_dense_rank_2"),
    )
)

scored = (
    metrics_price_tiers
    .select(
        "element",
        "element_dense_rank",
        "element_dense_rank_2",
        F.dense_rank().over(w4).alias("element_dense_rank_3"),
        F.count("currency_avg_element_dense_rank_2").over(w2).alias("currency_count_currency_avg_element_dense_rank_2"),
    )
)

result = (
    scored
    .select(
        "element_dense_rank",
        "element_dense_rank_2",
        "element_dense_rank_3",
        F.cume_dist().over(w5).alias("element_dense_rank_3_cume_dist"),
    )
)
