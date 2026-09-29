from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc(), "id")
w2 = Window.partitionBy("category", "supplier_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "stock_qty",
        "tags",
        F.row_number().over(w).alias("row_seq"),
        F.sum("price").over(w2).alias("cumulative_price"),
        F.last("cost", ignorenulls=True).over(w3).alias("last_known_cost"),
    )
)
