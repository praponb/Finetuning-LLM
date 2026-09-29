from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("store_id") == 1)
    .select(
        "id",
        "product_id",
        "store_id",
        "last_restock",
        "quantity",
        F.sum("quantity").over(w).alias("running_quantity"),
        F.row_number().over(w2).alias("row_num"),
    )
)
