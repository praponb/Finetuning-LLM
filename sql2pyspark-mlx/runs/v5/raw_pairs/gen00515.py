from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category").orderBy(F.col("stock_qty").desc(), "id")
w3 = Window.partitionBy("supplier_id").orderBy("cost", "id")

result = (
    df_products
    .filter(F.array_contains("tags", "clearance"))
    .select(
        "*",
        F.sum("cost").over(w).alias("cumulative_cost"),
        F.row_number().over(w2).alias("row_seq"),
        F.lag("price", 1, 0).over(w3).alias("prev_price"),
    )
)
