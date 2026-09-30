from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        F.sum("quantity").over(w).alias("cumulative_quantity"),
        F.sum("quantity").over(w2).alias("running_quantity"),
        F.avg("quantity").over(w3).alias("quantity_avg_2"),
    )
)
