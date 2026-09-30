from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_first())
w_running = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

result = (
    exploded
    .select(
        "*",
        F.rank().over(w_hist).alias("element_rank"),
        F.dense_rank().over(w_running).alias("element_dense_rank"),
        F.rank().over(w_running).alias("element_rank_2"),
    )
)
