from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "order_id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([2, 11, 28, 38]))
    .select(
        "id",
        "product_id",
        "quantity",
        F.max("quantity").over(w_running).alias("running_max_quantity"),
        F.count("quantity").over(w_running).alias("running_count_quantity"),
        F.avg("quantity").over(w_running).alias("running_avg_quantity"),
        F.sum("quantity").over(w_running).alias("running_sum_quantity"),
        F.min("quantity").over(w2).alias("product_id_min_quantity"),
    )
)
