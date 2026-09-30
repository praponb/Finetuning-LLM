from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("quantity", "id")
w3 = Window.partitionBy("product_id").orderBy("last_restock", "id")

with_windows = (
    df_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        F.last("quantity", ignorenulls=True).over(w).alias("last_known_quantity"),
        F.lead("quantity", 1).over(w2).alias("next_quantity"),
        F.lead("quantity", 1).over(w3).alias("next_quantity_2"),
    )
)

result = (
    with_windows
    .select("id", "store_id", "quantity", "last_known_quantity", "next_quantity")
    .orderBy(F.col("quantity").desc_nulls_first())
)
