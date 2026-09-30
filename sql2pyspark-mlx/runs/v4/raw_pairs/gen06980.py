from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").asc_nulls_first())
w2 = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_last())
w3 = Window.partitionBy("currency")

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

metrics = (
    exploded
    .select(
        "*",
        F.percent_rank().over(w).alias("element_percent_rank"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
    )
)

metrics_price_tiers = (
    metrics
    .withColumn("element_dense_rank_share", F.round((F.col("element_dense_rank") / F.sum("element_dense_rank").over(w3)), 4))
    .withColumn("currency_stddev_samp_element", F.stddev_samp("element").over(w3))
    .withColumn("currency_avg_element_dense_rank", F.avg("element_dense_rank").over(w3))
)

result = (
    metrics_price_tiers
    .select(
        "id",
        "effective_from",
        "element",
        "element_dense_rank",
        "currency_avg_element_dense_rank",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
