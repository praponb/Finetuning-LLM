from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 10]))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "quantity",
        (F.col("quantity") - F.lag("quantity", 1).over(w)).alias("quantity_change"),
    )
    .orderBy(F.col("product_id").desc())
)
