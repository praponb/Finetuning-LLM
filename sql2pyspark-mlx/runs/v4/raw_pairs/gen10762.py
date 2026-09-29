from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "unit_price",
        "quantity",
        F.lag("quantity", 1).over(w).alias("prev_quantity"),
    )
)
