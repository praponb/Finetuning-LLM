from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("quantity") != 227)
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.ntile(5).over(w).alias("quantity_bucket"),
        F.lead("quantity", 1).over(w).alias("next_quantity"),
        F.lead("quantity", 1).over(w2).alias("next_quantity_2"),
    )
)
