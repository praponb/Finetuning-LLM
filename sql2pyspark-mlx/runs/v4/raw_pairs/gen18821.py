from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        "product_id",
        F.first("product_id").over(w).alias("first_product_id"),
    )
    .orderBy("unit_price")
)
