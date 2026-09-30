from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("plan", "currency").orderBy("tier_value")
w2 = Window.orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value").desc_nulls_last())
w4 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

result = (
    lines
    .select(
        "*",
        F.dense_rank().over(w_hist).alias("tier_value_dense_rank"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
        F.dense_rank().over(w3).alias("tier_value_dense_rank_2"),
        F.dense_rank().over(w4).alias("tier_value_dense_rank_3"),
    )
)
