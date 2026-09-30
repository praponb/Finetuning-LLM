from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 5, 6]))
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity",
        F.count("*").over(w).alias("store_id_count"),
        F.sum("quantity").over(w2).alias("running_quantity"),
    )
    .orderBy(F.col("id").desc(), F.col("store_id_count").desc())
)
