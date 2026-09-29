from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_last())
w3 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-07-25", "2025-07-14"))
    .select(
        "id",
        "product_id",
        "quantity",
        F.lag("quantity", 2).over(w).alias("prev_quantity"),
        F.dense_rank().over(w2).alias("quantity_dense_rank"),
        F.sum("quantity").over(w3).alias("cumulative_quantity"),
    )
)
