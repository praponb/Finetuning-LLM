from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id")
w2 = Window.partitionBy("order_id", "product_id")

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select(
        "*",
        F.ntile(4).over(w).alias("quantity_bucket"),
        F.max("quantity").over(w2).alias("order_id_max_quantity"),
    )
)
