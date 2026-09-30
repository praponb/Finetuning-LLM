from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity",
        F.lag("quantity", 1, 0).over(w).alias("prev_quantity"),
    )
    .orderBy(F.col("last_restock").desc(), "id")
)
