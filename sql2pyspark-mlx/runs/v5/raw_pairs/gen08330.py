from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id")
w_ordered = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        "unit_price",
        F.sum("unit_price").over(w).alias("order_id_sum_unit_price"),
        F.lead("unit_price", 1, 0).over(w_ordered).alias("next_unit_price"),
    )
)
