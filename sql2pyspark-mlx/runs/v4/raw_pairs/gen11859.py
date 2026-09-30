from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("quantity") == 133)
    .select(
        "id",
        "last_restock",
        "quantity",
        (F.col("quantity") - F.lag("quantity", 1).over(w)).alias("quantity_change"),
    )
)
