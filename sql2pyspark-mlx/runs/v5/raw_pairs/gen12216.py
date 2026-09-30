from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id")

result = (
    df_order_items
    .select(
        "id",
        "unit_price",
        "quantity",
        F.sum("quantity").over(w).alias("running_quantity"),
    )
)
