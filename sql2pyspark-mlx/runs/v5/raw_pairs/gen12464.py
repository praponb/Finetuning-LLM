from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        "store_id",
        F.lead("quantity", 1).over(w).alias("next_quantity"),
        (F.col("quantity") - F.lag("quantity", 1).over(w2)).alias("quantity_change"),
    )
    .orderBy(F.col("store_id").desc(), F.col("next_quantity").desc())
)
