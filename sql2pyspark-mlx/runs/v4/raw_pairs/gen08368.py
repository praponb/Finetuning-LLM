from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("unit_price", "id")

result = (
    df_order_items
    .select(
        "id",
        "unit_price",
        F.round(F.avg("unit_price").over(w), 2).alias("unit_price_moving_avg_4"),
        F.ntile(3).over(w2).alias("bucket"),
    )
)
