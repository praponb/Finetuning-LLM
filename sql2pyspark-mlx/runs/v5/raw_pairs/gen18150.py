from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id")

result = (
    df_inventory
    .filter(F.month("last_restock").isin([4, 6, 8]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.row_number().over(w).alias("event_seq"),
        F.ntile(3).over(w2).alias("quantity_bucket"),
    )
)
