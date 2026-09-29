from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("category").orderBy("price", "id").rowsBetween(-3, Window.currentRow)
w_ordered = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_products
    .filter(F.col("launch_date").between("2023-12-01", "2025-03-01"))
    .select(
        "*",
        F.avg("stock_qty").over(w_running).alias("stock_qty_avg_3"),
        F.sum("stock_qty").over(w_ordered).alias("cumulative_stock_qty"),
    )
)

result = (
    scored
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "stock_qty_avg_3",
    )
)
