from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("category").orderBy(F.col("price").desc(), "id")
w2 = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("category").orderBy("stock_qty", "id")
w4 = Window.partitionBy("category", "supplier_id").orderBy("stock_qty", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "stock_qty",
        "tags",
        F.ntile(10).over(w_grp).alias("price_bucket"),
        F.sum("price").over(w2).alias("running_price"),
        F.lag("price", 1).over(w3).alias("prev_price"),
        F.avg("stock_qty").over(w4).alias("stock_qty_avg_3"),
    )
)
