from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.max("quantity").over(w).alias("last3_max_quantity"),
        F.sum("quantity").over(w).alias("last3_sum_quantity"),
        F.min("quantity").over(w).alias("last3_min_quantity"),
        F.avg("quantity").over(w2).alias("quantity_avg_2"),
    )
)
