from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock", "id")
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.lead("quantity", 1).over(w).alias("next_quantity"),
        F.sum("quantity").over(w2).alias("running_quantity"),
        F.sum("quantity").over(w3).alias("cumulative_quantity"),
    )
)

result = (
    with_windows
    .select("id", "store_id", "product_id", "next_quantity", "running_quantity")
)
