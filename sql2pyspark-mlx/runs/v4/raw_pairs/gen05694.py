from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 4, 5]))
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity",
        "product_id",
        F.lead("quantity", 2).over(w).alias("next_quantity"),
        F.sum("quantity").over(w2).alias("running_quantity"),
    )
)
