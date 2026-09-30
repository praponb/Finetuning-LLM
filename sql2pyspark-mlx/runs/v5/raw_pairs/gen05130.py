from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")
w2 = Window.orderBy("unit_price", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity",
        F.ntile(2).over(w).alias("quartile"),
        F.round(F.avg("quantity").over(w2), 2).alias("quantity_moving_avg_7"),
    )
    .orderBy("unit_price")
)
