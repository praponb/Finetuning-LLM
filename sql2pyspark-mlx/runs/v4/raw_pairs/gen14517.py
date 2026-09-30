from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_last())
w2 = Window.orderBy("element")
w3 = Window.orderBy(F.col("element").desc_nulls_first())

exploded = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("element"))
)

result = (
    exploded
    .select(
        "*",
        F.rank().over(w).alias("element_rank"),
        F.percent_rank().over(w2).alias("element_percent_rank"),
        F.dense_rank().over(w3).alias("element_dense_rank"),
    )
)
