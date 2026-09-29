from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 7, 9, 10]))
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity",
        F.lag("quantity", 1).over(w).alias("prev_quantity"),
    )
)
