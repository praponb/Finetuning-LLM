from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")

result = (
    df_order_items
    .filter(F.col("unit_price") < 360)
    .select(
        "id",
        "order_id",
        "quantity",
        F.sum("quantity").over(w).alias("order_id_sum_quantity"),
    )
)
