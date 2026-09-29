from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 4, 6]))
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity",
        "product_id",
        F.sum("quantity").over(w).alias("running_quantity"),
        F.last("quantity").over(w2).alias("last_quantity"),
    )
    .orderBy(F.col("store_id").desc())
)
