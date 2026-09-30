from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        F.sum("quantity").over(w).alias("running_quantity"),
        F.stddev_samp("quantity").over(w2).alias("product_id_stddev_samp_quantity"),
    )
)
