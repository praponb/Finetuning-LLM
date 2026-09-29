from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.row_number().over(w).alias("quantity_row"),
        F.sum("quantity").over(w2).alias("running_quantity"),
    )
)
