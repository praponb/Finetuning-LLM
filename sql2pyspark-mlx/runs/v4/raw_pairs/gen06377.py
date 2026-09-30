from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id")
w_ordered = Window.partitionBy("supplier_id")
w_grp = Window.partitionBy("supplier_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .select(
        "*",
        F.sum("price").over(w).alias("category_sum_price"),
        F.corr("price", "stock_qty").over(w_ordered).alias("corr_price_stock_qty"),
        F.sum("stock_qty").over(w_grp).alias("cumulative_stock_qty"),
    )
)
