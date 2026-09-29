from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id").orderBy("launch_date", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("supplier_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("supplier_id", "category").orderBy("price", "id")

result = (
    df_products
    .select(
        "*",
        F.avg("stock_qty").over(w).alias("stock_qty_avg_6_prior"),
        F.sum("cost").over(w2).alias("cumulative_cost"),
        F.ntile(4).over(w3).alias("price_bucket"),
    )
)
