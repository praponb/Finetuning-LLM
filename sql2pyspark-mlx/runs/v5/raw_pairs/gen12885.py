from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "unit_price",
        F.min("quantity").over(w_part).alias("running_min_quantity"),
        F.avg("quantity").over(w_part).alias("running_avg_quantity"),
        F.count("quantity").over(w_part).alias("running_count_quantity"),
        F.ntile(4).over(w2).alias("quantity_bucket"),
    )
)
