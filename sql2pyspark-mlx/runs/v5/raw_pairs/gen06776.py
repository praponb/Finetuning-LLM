from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        "store_id",
        "quantity",
        F.row_number().over(w).alias("last_restock_row"),
        F.row_number().over(w2).alias("rn"),
    )
)
