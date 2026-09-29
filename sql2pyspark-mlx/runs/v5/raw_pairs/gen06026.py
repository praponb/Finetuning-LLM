from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy(F.col("stock_qty").desc(), "id")
w2 = Window.partitionBy("brand").orderBy("stock_qty", "id")
w3 = Window.partitionBy("supplier_id")
w4 = Window.partitionBy("supplier_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        F.ntile(4).over(w).alias("stock_qty_bucket"),
        F.lead("price", 2).over(w2).alias("next_price"),
        F.corr("stock_qty", "price").over(w3).alias("corr_stock_qty_price"),
        F.sum("stock_qty").over(w4).alias("cumulative_stock_qty"),
    )
)
