from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock", "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("product_id", "store_id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.row_number().over(w).alias("seq_no"),
        F.avg("quantity").over(w2).alias("quantity_avg_2"),
        F.stddev_samp("quantity").over(w3).alias("product_id_stddev_samp_quantity"),
    )
)
