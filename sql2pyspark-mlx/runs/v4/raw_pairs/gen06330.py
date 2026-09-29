from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.orderBy("unit_price", "id")

result = (
    df_order_items
    .select(
        "*",
        F.ntile(10).over(w_ordered).alias("quantity_bucket"),
        F.row_number().over(w2).alias("event_seq"),
        F.ntile(3).over(w_ordered).alias("quantity_bucket_2"),
    )
)
