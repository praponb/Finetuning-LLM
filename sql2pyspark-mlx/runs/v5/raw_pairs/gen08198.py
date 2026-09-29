from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)
w_running = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id")
w4 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        F.avg("quantity").over(w_grp).alias("quantity_avg_3"),
        F.last("quantity", ignorenulls=True).over(w_running).alias("last_known_quantity"),
        F.lead("quantity", 1).over(w3).alias("next_quantity"),
        F.sum("quantity").over(w4).alias("running_quantity"),
        F.sum("quantity").over(w5).alias("running_quantity_2"),
    )
)
