from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("price", "id")

result = (
    df_products
    .select(
        "id",
        "brand",
        "launch_date",
        "stock_qty",
        "price",
        F.round(F.avg("stock_qty").over(w), 2).alias("stock_qty_moving_avg_4"),
        F.ntile(3).over(w2).alias("quartile"),
    )
)
