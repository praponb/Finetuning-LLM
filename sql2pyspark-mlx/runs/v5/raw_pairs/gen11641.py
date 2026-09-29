from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        "store_id",
        F.lead("quantity", 1, 0).over(w).alias("next_quantity"),
        F.lag("quantity", 1).over(w2).alias("prev_quantity"),
    )
)
