from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity",
        "product_id",
        F.lag("quantity", 1).over(w).alias("prev_quantity"),
        F.ntile(2).over(w2).alias("bucket"),
    )
    .orderBy(F.col("last_restock").desc())
)
