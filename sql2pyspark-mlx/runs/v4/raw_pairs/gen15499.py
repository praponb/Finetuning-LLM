from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        (F.col("quantity") - F.lag("quantity", 2, 0).over(w)).alias("quantity_change"),
    )
    .orderBy(F.col("quantity_change").desc())
)
