from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.ntile(4).over(w).alias("quantity_bucket"),
        F.lag("quantity", 1).over(w2).alias("prev_quantity"),
        F.ntile(5).over(w2).alias("quantity_bucket_2"),
    )
)
