from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id")
w3 = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id")
w4 = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc_nulls_last())

result = (
    df_inventory
    .select(
        "*",
        F.ntile(3).over(w).alias("quantity_bucket"),
        F.ntile(10).over(w2).alias("quantity_bucket_2"),
        F.ntile(5).over(w3).alias("quantity_bucket_3"),
        F.sum("quantity").over(w4).alias("cumulative_quantity"),
        F.dense_rank().over(w5).alias("quantity_dense_rank"),
    )
)
