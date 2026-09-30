from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc(), "id")

scored = (
    df_inventory
    .select(
        "*",
        F.ntile(10).over(w).alias("quantity_bucket"),
        F.first("quantity", ignorenulls=True).over(w2).alias("first_known_quantity"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

result = (
    scored
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "quantity_bucket",
        "first_known_quantity",
        "seq_no",
    )
)
