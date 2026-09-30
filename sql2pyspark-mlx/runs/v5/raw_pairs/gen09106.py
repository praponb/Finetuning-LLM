from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_inventory
    .select(
        "id",
        "quantity",
        F.count("quantity").over(w_running).alias("last4_count_quantity"),
        F.avg("quantity").over(w_running).alias("last4_avg_quantity"),
        F.min("quantity").over(w_running).alias("last4_min_quantity"),
    )
)
