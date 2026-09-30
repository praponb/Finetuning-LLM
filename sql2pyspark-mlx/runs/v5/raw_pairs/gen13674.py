from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .filter(F.col("unit_price") < 130)
    .select(
        "*",
        F.sum("quantity").over(w).alias("running_sum_quantity"),
        F.avg("quantity").over(w).alias("running_avg_quantity"),
        F.max("quantity").over(w).alias("running_max_quantity"),
        F.count("quantity").over(w).alias("running_count_quantity"),
        F.last("quantity", ignorenulls=True).over(w2).alias("last_known_quantity"),
    )
)
