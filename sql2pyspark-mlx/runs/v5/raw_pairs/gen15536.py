from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("brand").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "launch_date",
        "price",
        "brand",
        "stock_qty",
        F.sum("price").over(w).alias("running_price"),
        (F.col("stock_qty") - F.lag("stock_qty", 2).over(w2)).alias("stock_qty_change"),
    )
    .orderBy(F.col("stock_qty_change").desc())
)
