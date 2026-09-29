from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-05-27", "2024-05-27"))
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.row_number().over(w).alias("row_num"),
        (F.col("quantity") - F.lag("quantity", 1).over(w)).alias("quantity_change"),
    )
    .orderBy(F.col("quantity").desc())
)
