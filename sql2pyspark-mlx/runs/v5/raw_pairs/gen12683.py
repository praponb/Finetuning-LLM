from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("qty").desc_nulls_last())
w2 = Window.partitionBy("promo").orderBy(F.col("qty").asc_nulls_last())

lines = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

result = (
    lines
    .select(
        "id",
        "product_id",
        F.cume_dist().over(w).alias("qty_cume_dist"),
        F.rank().over(w2).alias("qty_rank"),
    )
)
