from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_last())
w2 = Window.partitionBy("currency").orderBy("element")
w_recent = Window.partitionBy("currency")

exploded = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("element"))
)

result = (
    exploded
    .select(
        "*",
        F.dense_rank().over(w).alias("element_dense_rank"),
        F.dense_rank().over(w2).alias("element_dense_rank_2"),
        F.min("element").over(w_recent).alias("currency_min_element"),
        F.count("element").over(w_recent).alias("currency_count_element"),
    )
)
