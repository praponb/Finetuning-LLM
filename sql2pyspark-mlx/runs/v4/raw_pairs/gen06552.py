from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("supplier_id")

with_windows = (
    df_products
    .select(
        "*",
        F.sum("stock_qty").over(w).alias("running_stock_qty"),
        F.avg("stock_qty").over(w2).alias("stock_qty_avg_6"),
        F.corr("stock_qty", "price").over(w3).alias("corr_stock_qty_price"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "running_stock_qty",
        "corr_stock_qty_price",
    )
    .orderBy(F.col("stock_qty").desc(), F.col("corr_stock_qty_price").desc())
)
