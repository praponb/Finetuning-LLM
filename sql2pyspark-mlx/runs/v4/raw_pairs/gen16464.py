from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id")
w3 = Window.partitionBy("brand")
w4 = Window.partitionBy("brand").orderBy("price", "id")
w5 = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .select(
        "*",
        F.first("stock_qty", ignorenulls=True).over(w).alias("first_known_stock_qty"),
        F.corr("price", "cost").over(w2).alias("corr_price_cost"),
        F.corr("cost", "stock_qty").over(w3).alias("corr_cost_stock_qty"),
        F.lag("stock_qty", 1).over(w4).alias("prev_stock_qty"),
        F.sum("cost").over(w5).alias("cumulative_cost"),
    )
)
