from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")
w2 = Window.orderBy("last_restock", "id")

with_windows = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.lead("quantity", 1).over(w).alias("next_quantity"),
        F.lead("quantity", 1).over(w2).alias("next_quantity_2"),
    )
)

result = (
    with_windows
    .select("id", "product_id", "quantity", "next_quantity", "next_quantity_2")
    .orderBy(F.col("next_quantity").desc())
)
