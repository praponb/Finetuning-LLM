from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")
w2 = Window.orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.first("last_restock").over(w).alias("first_last_restock"),
        F.ntile(10).over(w2).alias("quartile"),
    )
)
