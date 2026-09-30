from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc_nulls_last())
w2 = Window.orderBy(F.col("element").desc())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("discounts").alias("element"))
)

result = (
    lines
    .select(
        "id",
        "effective_from",
        F.rank().over(w).alias("element_rank"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
    )
)
