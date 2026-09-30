from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first())
w2 = Window.partitionBy("store_id")
w3 = Window.orderBy("last_restock", "id")
w4 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .select(
        "*",
        F.cume_dist().over(w).alias("quantity_cume_dist"),
        F.min("quantity").over(w2).alias("store_id_min_quantity"),
        F.lag("quantity", 2).over(w3).alias("prev_quantity"),
        F.sum("quantity").over(w4).alias("cumulative_quantity"),
        F.sum("quantity").over(w5).alias("running_quantity"),
    )
)
