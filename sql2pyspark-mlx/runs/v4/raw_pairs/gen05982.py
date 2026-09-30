from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w4 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .select(
        "*",
        F.min("quantity").over(w).alias("store_id_min_quantity"),
        F.ntile(5).over(w2).alias("quantity_bucket"),
        F.ntile(4).over(w3).alias("quantity_bucket_2"),
        F.sum("quantity").over(w4).alias("cumulative_quantity"),
    )
)
