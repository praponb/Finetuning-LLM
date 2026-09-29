from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc())
w_part = Window.partitionBy("store_id")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.dense_rank().over(w).alias("quantity_dense_rank"),
        F.max("quantity").over(w_part).alias("store_id_max_quantity"),
    )
)
