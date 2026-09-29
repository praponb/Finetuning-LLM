from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .filter(F.col("quantity") != 127)
    .select(
        "id",
        "store_id",
        "quantity",
        F.min("quantity").over(w_hist).alias("running_min_quantity"),
        F.sum("quantity").over(w_hist).alias("running_sum_quantity"),
        F.max("quantity").over(w_hist).alias("running_max_quantity"),
        F.sum("quantity").over(w_ordered).alias("running_quantity"),
    )
)
