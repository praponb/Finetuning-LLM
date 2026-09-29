from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        "product_id",
        "unit_price",
        F.round(F.avg("quantity").over(w), 2).alias("quantity_moving_avg_4"),
        F.sum("unit_price").over(w2).alias("running_unit_price"),
    )
)
