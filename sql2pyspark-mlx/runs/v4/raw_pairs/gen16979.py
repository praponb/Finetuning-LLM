from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity",
        F.lead("quantity", 1).over(w).alias("next_quantity"),
        F.dense_rank().over(w2).alias("last_restock_dense_rank"),
    )
)
