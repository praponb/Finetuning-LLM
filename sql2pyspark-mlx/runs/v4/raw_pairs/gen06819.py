from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.sum("quantity").over(w).alias("store_id_sum_quantity"),
        F.lead("quantity", 1).over(w2).alias("next_quantity"),
    )
)
