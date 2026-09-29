from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "*",
        F.first("quantity", ignorenulls=True).over(w_part).alias("first_known_quantity"),
        F.last("quantity", ignorenulls=True).over(w2).alias("last_known_quantity"),
        F.lead("quantity", 1).over(w3).alias("next_quantity"),
    )
)
