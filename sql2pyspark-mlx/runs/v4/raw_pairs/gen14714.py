from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_last())

result = (
    df_inventory
    .select(
        "*",
        F.ntile(5).over(w).alias("quantity_bucket"),
        F.dense_rank().over(w2).alias("quantity_dense_rank"),
    )
)
