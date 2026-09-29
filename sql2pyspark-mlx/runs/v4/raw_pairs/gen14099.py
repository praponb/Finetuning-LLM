from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "unit_price",
        "quantity",
        F.min("unit_price").over(w).alias("product_id_min_unit_price"),
        F.sum("quantity").over(w).alias("product_id_sum_quantity"),
    )
)
