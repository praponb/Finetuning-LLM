from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .filter(F.col("store_id") == 2)
    .select(
        "id",
        "product_id",
        "store_id",
        "quantity",
        F.sum("quantity").over(w).alias("product_id_sum_quantity"),
        F.dense_rank().over(w2).alias("quantity_dense_rank"),
    )
)
