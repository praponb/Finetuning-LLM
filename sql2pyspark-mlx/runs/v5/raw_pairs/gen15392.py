from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy("launch_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "category",
        "launch_date",
        "stock_qty",
        F.round(F.avg("stock_qty").over(w), 2).alias("stock_qty_moving_avg_4"),
    )
)
