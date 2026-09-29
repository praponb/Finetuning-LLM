from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        F.round(F.avg("quantity").over(w), 2).alias("quantity_moving_avg_4"),
    )
    .orderBy("quantity")
)
