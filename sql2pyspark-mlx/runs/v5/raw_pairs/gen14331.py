from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy("element")
w3 = Window.orderBy("element_dense_rank")
w4 = Window.orderBy(F.col("element_dense_rank").desc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

scored = (
    exploded
    .select(
        "id",
        "effective_from",
        F.max("element").over(w).alias("currency_max_element"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "currency_max_element",
        "element_dense_rank",
        F.percent_rank().over(w3).alias("element_dense_rank_percent_rank"),
        F.dense_rank().over(w4).alias("element_dense_rank_dense_rank"),
    )
)

valid = (
    with_windows
    .filter(F.col("element_dense_rank_dense_rank").between(3, 8))
    .select(
        "*",
        (F.col("currency_max_element") + F.col("element_dense_rank_dense_rank")).alias("currency_max_element_plus_element_dense_rank_dense_rank"),
        F.ceil("element_dense_rank").alias("element_dense_rank_whole"),
    )
)

result = (
    valid
    .select(
        "id",
        "currency_max_element",
        "element_dense_rank",
        "currency_max_element_plus_element_dense_rank_dense_rank",
    )
    .orderBy(
        F.col("currency_max_element_plus_element_dense_rank_dense_rank").desc_nulls_first(),
    )
)
