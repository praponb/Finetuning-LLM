from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("price")
w2 = Window.orderBy(F.col("qty").desc_nulls_last())
w3 = Window.orderBy(F.col("price").desc())
w4 = Window.orderBy(F.col("product_id").desc())

lines = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select("id", F.col("elem.product_id"), F.col("elem.qty"), F.col("elem.price"))
)

result = (
    lines
    .select(
        "*",
        F.cume_dist().over(w).alias("price_cume_dist"),
        F.dense_rank().over(w2).alias("qty_dense_rank"),
        F.dense_rank().over(w3).alias("price_dense_rank"),
        F.percent_rank().over(w4).alias("product_id_percent_rank"),
        F.percent_rank().over(w2).alias("qty_percent_rank"),
    )
)
