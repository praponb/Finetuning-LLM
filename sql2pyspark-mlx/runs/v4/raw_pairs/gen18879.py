from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").desc(), "id")
w2 = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("supplier_id")
w4 = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .select(
        "*",
        F.ntile(3).over(w).alias("price_bucket"),
        F.last("stock_qty", ignorenulls=True).over(w2).alias("last_known_stock_qty"),
        F.min("cost").over(w3).alias("supplier_id_min_cost"),
        F.sum("stock_qty").over(w4).alias("cumulative_stock_qty"),
    )
)
