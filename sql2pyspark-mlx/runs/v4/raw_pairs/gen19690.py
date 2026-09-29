from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_first())
w2 = Window.orderBy(F.col("sqft").desc_nulls_last())
w3 = Window.orderBy(F.col("sqft").asc_nulls_first())
w4 = Window.partitionBy("region")

result = (
    df_stores
    .select(
        "*",
        F.dense_rank().over(w_recent).alias("sqft_dense_rank"),
        F.dense_rank().over(w2).alias("sqft_dense_rank_2"),
        F.cume_dist().over(w3).alias("sqft_cume_dist"),
        F.round((F.col("sqft") / F.sum("sqft").over(w4)), 4).alias("sqft_share"),
    )
)
