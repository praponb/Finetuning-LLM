from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("store_id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        F.sum("quantity").over(w).alias("last4_sum_quantity"),
        F.count("quantity").over(w).alias("last4_count_quantity"),
        F.max("quantity").over(w).alias("last4_max_quantity"),
        F.max("quantity").over(w2).alias("store_id_max_quantity"),
    )
)
