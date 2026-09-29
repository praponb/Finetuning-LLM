from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id")
w3 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc(), "id")
w4 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.rank().over(w).alias("quantity_rank"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("row_seq_2"),
        F.sum("quantity").over(w4).alias("running_quantity"),
    )
)
